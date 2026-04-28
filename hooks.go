package orderflow

import "context"

// 本文件声明 Config 里会用到的函数钩子类型。
//
// 约定：
//   - 返回 error 的钩子失败会阻断主流程（Create / HandleNotify 将返回该错误）；
//   - 不返回 error 的钩子为"旁路观察"类（记日志 / 发告警），失败不影响主流程。

// OnCreatedHook 在订单创建并落库后触发。
type OnCreatedHook[O OrderSnapshot] func(ctx context.Context, order O) error

// OnPaidHook 在订单被确认支付成功、进入履约阶段时触发。
// 典型用法：根据 order 的权益快照发放 VIP / 实物发货 / 积分入账。
//
// **同步执行约束**：钩子必须在返回前完成所有副作用。Engine 在钩子返回 nil 时认为履约
// 已成功并推进状态（Paid → Delivered），返回 error 时触发补偿重试。钩子内部如果启动
// 子 goroutine 做异步 I/O 并立即返回 nil，会导致：
//
//  1. Engine 以为履约已成功但子 goroutine 可能正在失败，订单状态与业务副作用不一致；
//  2. 进程关停时 Engine 不会 wait 业务 goroutine，异步任务被强杀；
//  3. 异步错误发生时没有机制回滚订单状态或触发 fallback 重试。
//
// 如果业务确实需要异步，钩子内部应自行管理 goroutine 生命周期 + 自己的幂等/重试/告警
// 机制，不要依赖 Engine 的补偿路径。
//
// **幂等性强约束**：此钩子可能被多次调用（同一 (orderNo, tradeNo) 元组）。
// Engine 在以下场景会重入 OnPaid：
//
//  1. Store.FinalizePaidOrder 失败（DB 瞬时不可用、事务 deadlock）后，
//     DeliveryFallback 周期扫描 Paid-undelivered 并调用 ReconcilePaid；
//  2. 支付网关在极端网络场景下的重复回调；
//  3. "关闭后又收到支付成功"竞态经 CASReopenPaid 恢复到 Paid 后重新 finalize。
//
// **幂等实现准则**（业务方必须遵守）：
//
//   - **以 orderNo 为幂等键**，不要用 tradeNo 或 时间戳；同一订单的 OnPaid 多次调用
//     必须产生相同的副作用（例如 VIP 到期时间绑定 orderNo，重复执行直接返回成功）。
//   - 副作用必须写入同一份数据源并带唯一约束（`UNIQUE INDEX uk_order_no`）；
//     依赖内存去重 / 基于时间戳去重都不可靠。
//   - 如果副作用调用第三方 API（发短信、发 VIP 到外部系统），在调用前必须有本地
//     "已完成" 标记检查——幂等表 / 订单状态字段 / Redis SetNX 都可以。
//
// 返回错误会触发 Engine 的补偿路径（AnomalyDeliveryFailed + fallback worker 重试），
// 订单状态停留在 Paid 不会推进到 Delivered。
type OnPaidHook[O OrderSnapshot] func(ctx context.Context, order O, notify NotifyResult) error

// OnDeliveredHook 在订单履约完成、状态迁到 Delivered 后触发。
type OnDeliveredHook[O OrderSnapshot] func(ctx context.Context, order O) error

// OnClosedHook 订单关闭后触发。reason 给出关闭原因。
type OnClosedHook[O OrderSnapshot] func(ctx context.Context, order O, reason ClosedReason)

// OnReopenedHook 已关闭的订单被支付网关确认成功、经 CASReopenPaid 恢复后触发。
//
// **⚠ 重要：此钩子触发后 Engine 会立刻调用 OnPaid + OnDelivered**
// （见 engine_notify.go 的 handleClosedPaidNotify 时序）。业务方**不要在此钩子内做任何
// "发权益"类副作用**——否则同一订单会先在 OnReopened 发一次、再在 OnPaid 发一次，
// 造成**双倍发放**。
//
// 推荐分工：
//
//   - OnReopened：仅做"事件通知 / 审计 / 告警"——发企业微信、写审计日志、增 Prometheus
//     counter。Closed→Paid 在生产中是**异常恢复路径**，运维需要感知。
//   - OnPaid：所有"发权益"类副作用的**唯一入口**（VIP / 实物发货 / 积分入账），
//     并且必须幂等（见 OnPaidHook 的幂等强约束）。
//
// 若业务确实需要在恢复路径做特殊处理（如给"补偿支付"用户单独发优惠券），
// 也应封装为与 OnPaid 副作用解耦的独立操作，并自行做幂等去重。
type OnReopenedHook[O OrderSnapshot] func(ctx context.Context, order O, notify NotifyResult)

// OnSupersededHook 旧 Pending 订单被同用户的新订单替代关闭时触发。
type OnSupersededHook[O OrderSnapshot] func(ctx context.Context, old O, newProductID uint64)

// OnAnomalyHook 检测到业务异常时触发（金额不一致、状态机例外等）。
type OnAnomalyHook[O OrderSnapshot] func(ctx context.Context, order O, kind AnomalyKind, detail string)

// ResolveChannelFunc 将业务语义的支付方式（PayMethod typed enum）映射为网关渠道。
type ResolveChannelFunc func(payMethod PayMethod) Channel

// BuildNotifyURLFunc 根据渠道拼接支付回调 URL（允许带域名前缀 / 路径模板）。
type BuildNotifyURLFunc func(ch Channel) string

// IsReusableFunc 判断已存在的 Pending 订单能否被复用。
// 默认实现（Engine 未注入时）：价格一致 + 支付方式一致 -> 复用。
type IsReusableFunc[O OrderSnapshot] func(existing O, req CreateRequest) bool

// GenerateOrderNoFunc 生成订单号。默认实现基于 UTC 毫秒时间戳 + 随机后缀。
type GenerateOrderNoFunc func() string

// GenerateOrderTokenFunc 生成订单对外暴露的 token。
//
// **返回值约束**：
//   - 必须在 DB 列约束内（gormstore 默认 varchar(64)）；
//   - 必须是 URL-safe 字符集（token 可能出现在 URL path 或 query 里）；
//   - 必须不可预测——即使攻击者知道 orderNo / userID / productID 也不能重算 token。
//     入参作为建议信息保留在签名里，业务可用 HMAC(serverSecret, 三元组) 实现确定性 token，
//     但必须注入服务端 secret；**不要直接对三元组做哈希**——任何能从日志/对账单拿到
//     三元组的人都能离线算出 token，PollStatus / Subscribe 鉴权沦为摆设。
//
// 默认实现：16 字节 crypto/rand 的 hex 编码（128 bit 熵），忽略入参。
type GenerateOrderTokenFunc func(orderNo string, userID int64, productID uint64) string
