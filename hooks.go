package orderflow

import "context"

// 本文件声明 Config 里会用到的函数钩子类型。
//
// 约定：error 返回值的处理策略**因钩子而异**，并非"返回 error 就阻断主流程"。
//
//   - OnPaidHook：返回 error **阻断 finalize**——订单停在 Paid，FinalizePaidOrder 不执行，
//     由 DeliveryFallback 周期重试。这是核心包**唯一**会因钩子错误阻断主流程的位置。
//   - OnCreatedHook：返回 error 仅 logger.Warn，**不阻断** Create 主流程。订单已落库 +
//     入延时队列；钩子失败不会回滚订单，Engine 继续走 requestPayment。
//   - OnDeliveredHook：返回 error 仅 logger.Warn，**不阻断**。Delivered 状态已落库，
//     钩子失败只是旁路观察缺失，不影响订单生命周期。
//   - OnClosedHook / OnCancelledHook / OnReopenedHook / OnSupersededHook：不返回 error，
//     纯旁路观察。panic 会被 recover 吞掉（记 ALERT 日志）。
//   - OnAnomalyHook：不返回 error，纯告警出口。
//
// 设计取舍：OnPaid 阻断是为了保证"业务侧发放的权益 vs DB 履约状态"语义对齐——
// 业务侧发券失败时订单不能进入 Delivered。其他 hook 失败时订单状态已经稳定，没必要
// 因为业务旁路问题让用户重试或让 Engine 报错。

// OnCreatedHook 在订单创建并落库后触发。
//
// 返回 error 仅 logger.Warn，不阻断 Create 主流程。订单已落库 + 入延时队列，
// 钩子失败时业务侧应该在自己的实现里告警 / 入消息队列重试，Engine 层不会回滚订单。
type OnCreatedHook[O OrderSnapshot] func(ctx context.Context, order O) error

// OnPaidHook 在订单被确认支付成功、进入履约阶段时触发。
// 典型用法：根据 order 的权益快照发放 VIP / 实物发货 / 积分入账。
//
// **⚠ 事务边界**：钩子在 Store.FinalizePaidOrder 的事务**之外**执行（先调 OnPaid，
// 再开事务 finalize）。这是导致幂等约束的根因——OnPaid 已成功但 FinalizePaidOrder
// 事务因瞬时故障回滚时，业务侧已发放的权益不会随事务回滚，DeliveryFallback 周期重入
// 时会再次调用本钩子。业务方**必须**自行做幂等去重，否则会双倍发放（推荐用
// `rediscache.IdempotentOnPaidViaRedis` 包装本钩子，或注入业务侧自有的幂等表）。
//
// **想要事务内原子 DB 操作？**用 driver 提供的事务内钩子，**不要**塞进 OnPaid。
// 例如 `gormstore.Config.FinalizeExtra`：在 `FinalizePaidOrder` 同事务内调用，
// 接收 `*gorm.DB` 事务句柄，与订单状态推进、账单写入一起原子化。典型场景：更新
// `user_membership` 到期时间、扣减 `inventory` 库存、写入业务侧 audit 表等本地
// DB 副作用。OnPaid 只适合处理**天生不能在事务内执行**的副作用——外部 HTTP API
// （发短信 / 推送 / 第三方权益系统）会让事务持锁时间不可控，且外部失败时事务回滚
// 但已发请求无法回滚（这就是为什么 OnPaid 必须自己做幂等而不是依赖事务）。
//
// 速查：
//
//	事务内本地 DB 操作      → driver 的 FinalizeExtra（强一致，失败回滚）
//	外部 API / 跨系统副作用 → OnPaid + 自带幂等保护（最终一致，失败重试）
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
//
// 返回 error 仅 logger.Warn，**不阻断** finalize 主流程。Delivered 状态已落库，
// 钩子失败只是旁路观察缺失（典型场景：发送"履约完成"消息推送失败），不影响订单
// 生命周期。
type OnDeliveredHook[O OrderSnapshot] func(ctx context.Context, order O) error

// OnClosedHook 订单关闭后触发。reason 给出关闭原因。
type OnClosedHook[O OrderSnapshot] func(ctx context.Context, order O, reason ClosedReason)

// OnCancelledHook 订单被用户主动取消后触发（CancelByUser 推进的 StatusCancelled 路径）。
//
// 与 OnClosed 的区别：
//
//   - OnClosed：订单进入 StatusClosed（系统超时 / 管理员强制 / 被新订单取代 / 入队失败兜底）；
//   - OnCancelled：订单进入 StatusCancelled（用户主动放弃支付）。
//
// 业务侧关心"用户操作型订单终止"时实现本钩子（如重新发起 Pending 订单的引导、向用户征集
// 取消理由的反馈表单等）；关心"系统型订单终止"时实现 OnClosed。
//
// reason 是 CancelByUser 调用方传入的业务原因（如 "user_cancelled" / "switch_payment"），
// Engine 不解析它，仅原样转发到本钩子与 audit log。
type OnCancelledHook[O OrderSnapshot] func(ctx context.Context, order O, reason string)

// OnPaidAfterCancelledHook 在已取消订单被网关确认已支付后触发。
//
// Engine 在此路径只做事实确认与告警：订单保持 StatusCancelled，不恢复、不履约，也不主动
// 调 RefundGateway.Refund。调用方必须在本钩子内进入自己的幂等退款 outbox、对账工单或人工
// 处理流程。退款单号 / outbox 建议以 (orderNo, notify.TransactionID) 为唯一键，避免网关
// 重复回调、进程重启或多实例并发导致重复退款。
//
// 本钩子不返回 error。退款失败不能改变支付网关通知 ACK 语义；业务方应在自己的 outbox /
// worker / 告警系统中重试并追踪失败。
//
// 触发顺序：Engine 已先追加 Cancelled -> Cancelled 流水并触发 AnomalyPaidOnCancelled，
// 因此钩子内查询订单流水时可以看到这条审计记录。panic 会被 safeHook recover。
//
// # Engine 内部去重的边界（业务方必读）
//
// Engine 在调用本钩子前会做一层"进程内 FIFO 去重"——以 (orderNo, tradeNo) 为键、上限
// 4096 条、按入队顺序淘汰旧条目。该机制**仅能**抵御以下场景：
//
//   - 同一进程实例上的回调重放（典型：网关短时间重复推送）；
//   - 同一进程内首次查询失败、第二次重试到达。
//
// 它**无法**抵御以下场景，业务方必须自行实现跨实例幂等：
//
//   - **多实例部署**：每个进程的去重 map 独立。同一回调被 LB 分发到不同实例时，
//     各实例都会判定为"首次"并独立调用本钩子；
//   - **进程重启**：map 在内存中，重启后丢失。重启前已处理过的回调若再次到达会被视为首次；
//   - **极端高并发挤兑**：4096 条 FIFO 上限被填满后，最旧条目会被淘汰，理论上极旧
//     回调重新到达时会被视为首次（生产中极罕见，但不可忽视）。
//
// 因此**业务方必须**在钩子内基于 (orderNo, tradeNo) 用 Redis SETNX、DB 唯一索引或
// 业务自有持久化幂等表自行去重；**不能**依赖 Engine 的内存去重作为生产幂等保证——
// 它只是减少同实例下游噪声的"尽力而为"机制。
type OnPaidAfterCancelledHook[O OrderSnapshot] func(ctx context.Context, order O, notify NotifyResult)

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

// OnSupersededGatewayCloseFailedHook 在 SupersededDegraded 模式下，本地 CAS Close 成功
// 但网关 CloseOrder 全部重试仍失败时触发。
//
// # 触发条件（必须三条同时成立）
//
//  1. Config.CloseSupersededPolicy == SupersededDegraded
//  2. afterClose（含 3 次重试 + IsIgnorableCloseError）返回 err != nil
//  3. Store.CASClose 实际把旧单推到 StatusClosed（affected > 0）
//
// 缺少任一条都不会触发。例如 CAS race 失败（affected=0，订单被并发推到 Paid）时
// 旧单已不再是 Closed，本 hook 不应再 fire。
//
// # 业务方典型用法
//
//   - 把 oldOrderNo 推到自定义重试队列（K8s Job / 业务消息队列）周期重调 gateway.CloseOrder；
//   - Slack / 钉钉 / 飞书告警让人工感知"用户可能用旧 prepay_id 继续支付"的风险窗口；
//   - 写入风控审计表，便于事后追溯。
//
// # 注意
//
// 本 hook 是**旁路观察**（返回 void 而非 error）——失败不会回滚本地 CAS Close，
// 也不会阻塞新单创建。Engine 已经把旧单状态推到 Closed，hook 仅是给业务方一个
// 自主补救的扩展点。panic 会被 safeHook recover，不会冲破 Engine。
type OnSupersededGatewayCloseFailedHook[O OrderSnapshot] func(
	ctx context.Context, old O, newProductID uint64, gatewayErr error,
)

// OnAnomalyHook 检测到业务异常时触发（金额不一致、状态机例外等）。
type OnAnomalyHook[O OrderSnapshot] func(ctx context.Context, order O, kind AnomalyKind, detail string)

// ResolveChannelFunc 将业务语义的支付方式（PayMethod typed enum）映射为网关渠道。
type ResolveChannelFunc func(payMethod PayMethod) Channel

// BuildNotifyURLFunc 根据渠道拼接支付回调 URL（允许带域名前缀 / 路径模板）。
type BuildNotifyURLFunc func(ch Channel) string

// IsReusableFunc 判断已存在的 Pending 订单能否被复用。
// 默认实现（Engine 未注入时）：价格一致 + 支付方式一致 -> 复用。
type IsReusableFunc[O OrderSnapshot] func(existing O, req CreateRequest) bool

// GenerateOrderNoFunc 生成订单号。
//
// 入参 userID 是创建本次订单的用户 ID（来自 CreateRequest.UserID，鉴权后身份）。
// 业务方可基于 userID 拼接订单号（典型场景：从 PHP 等历史系统迁移过来要求订单号
// 前缀含用户 ID）。默认实现忽略 userID，基于 UTC 毫秒时间戳 + 随机后缀生成。
//
// 调用时机：Engine.Create 在锁外、CAS 写订单表前调用一次；返回值会写入
// OrderSpec.OrderNo（由 driver 持久化）。同一用户同一商品并发下单时（仍在
// `(user_id, product_id)` 维度上串行），各自调用一次拿到不同订单号。
type GenerateOrderNoFunc func(userID int64) string

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
