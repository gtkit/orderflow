// Package orderflow 提供可复用的订单流程引擎。
//
// 订单全生命周期（创建、超时关闭、支付回调、履约交付、状态推送、幂等补偿）被抽象到
// Engine[O] 之中；业务方只需要：
//
//  1. 让自己的订单结构体实现 OrderSnapshot 接口；
//  2. 通过 Store / PaymentGateway / DelayQueue / StatusCache / StatusStream
//     五个能力接口注入基础设施（DB / 支付网关 / Redis 等）；
//  3. 用函数钩子（OnPaid / OnClosed / OnAnomaly …）承载业务决策，
//     比如"支付成功后发放 VIP 权益"。
//
// 核心包对 gorm / go-redis / go-pay 等第三方库零依赖，基础设施实现封装在 drivers 子包里。
//
// # 钩子错误处理策略
//
// 不同钩子的错误处理策略不同，接入方必须清楚各自的语义：
//
//	钩子             | 签名返回      | 失败处理
//	-----------------|---------------|------------------------------------------
//	OnCreated        | error         | WARN 日志，不阻断 Create 主流程
//	OnPaid           | error         | 阻断 finalize 流程，订单停在 Paid，由 fallback worker 重试（必须幂等）
//	OnDelivered      | error         | WARN 日志，不阻断，订单已 Delivered
//	OnClosed         | (none)        | 旁路观察，无错误返回
//	OnReopened       | (none)        | 旁路观察；注意此时 finalize 尚未开始，可能后续仍失败
//	OnSuperseded     | (none)        | 旁路观察
//	OnAnomaly        | (none)        | 旁路观察，用于告警/审计
//
// 详细约束（特别是 OnPaid 的幂等要求）见 hooks.go 的各钩子类型定义。
//
// # 履约时序
//
// 支付成功时的推进顺序（HandleNotify / ReconcilePaid 共用）：
//
//  1. Store.CASConfirmPaid：Pending → Paid（或 ReconcilePaid 时 order 已在 Paid）
//  2. 状态广播：Cache.Set(Paid) + Stream.Publish(Paid)
//  3. OnPaid(ctx, order, notify)：业务侧发放权益（**必须幂等**）
//  4. Store.FinalizePaidOrder：Paid → Delivered + 写账单（同事务，可含 FinalizeExtra 钩子）
//  5. 状态广播：Cache.Set(Delivered) + Stream.Publish(Delivered)
//  6. OnDelivered(ctx, order)：旁路通知
//
// 步骤 3 失败 → 订单停在 Paid，AnomalyDeliveryFailed，fallback worker 周期重试。
// 步骤 4 失败 → 同上；权益可能已被 3 发放过一次，所以 OnPaid 必须幂等。
//
// # 生产部署清单
//
//   - Redis 集群部署：rediszq driver 的 key 必须用 hash tag（见 drivers/rediszq/doc.go）。
//   - worker goroutine：业务钩子 panic 会被 recover 吞掉，但必须检查日志关键字
//     "orderflow: panic in ... recovered" 做告警。
//   - "orderflow: ALERT ..." 开头的 ERROR 日志都值得配告警（异常订单、缓存不一致等）。
//   - drivers 的 go.mod replace 指令仅用于本地开发，发版前必须删除并 require 真实 tag
//     （仓库提供 scripts/check-release.sh 做 CI 校验）。
//
// # 调用方鉴权责任（业务方必读）
//
// Engine 不做用户身份鉴权，假设调用方已完成：
//
//   - **CreateRequest.UserID 必须来自鉴权后的身份上下文**，不得从 HTTP body / query /
//     header 直接读取。否则攻击者可传入他人 UserID，通过 FindPendingByUserAndProduct
//     找到受害者的 Pending 订单并经 closeSuperseded 关闭（DoS）。
//   - **PollStatus / Timeline 的 userID 参数**同样必须来自鉴权上下文。两方法内部基于
//     此参数做归属校验（ErrOrderForbidden）；若业务把 HTTP query userID 直接透传，
//     校验形同虚设。
//   - **Close(orderNo)** 不做 UserID 归属校验，仅供后台/worker 调用。如果要开放给用户
//     "主动取消订单"，业务方应自己先 GetByToken 拿 order 验 UserID 再调 Close。
//   - **HandleNotify 不依赖用户身份**——但要求 PaymentGateway.ParseNotify 完成验签
//     （见 gateway.go 的 PaymentGateway 接口文档）。
//
// # 日志与敏感信息
//
// Engine 会把 order_no / trade_no / user_id / amount / order_token 写进结构化日志。
// 这些字段在 PCI-DSS / 个人信息保护法等框架下可能属于敏感数据。建议：
//
//   - 使用 slog.JSONHandler 或标准 slog.TextHandler——它们会转义换行/引号，避免
//     攻击者通过 TradeNo 注入假日志行；
//   - 若需脱敏，在业务层自定义 slog.Handler 的 ReplaceAttr 钩子统一处理敏感字段；
//   - 日志收集链路（Kafka / ES）的访问权限按最小原则控制。
package orderflow
