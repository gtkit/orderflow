package orderflow

// ClosedReason 表示订单关闭原因，记录在日志 / OnClosed 钩子的参数中。
type ClosedReason string

const (
	// ClosedReasonTimeout 支付超时到期。
	ClosedReasonTimeout ClosedReason = "timeout"
	// ClosedReasonSuperseded 被同用户的新订单替代。
	ClosedReasonSuperseded ClosedReason = "superseded"
	// ClosedReasonManual 管理员 / 用户主动关闭。
	ClosedReasonManual ClosedReason = "manual"
	// ClosedReasonEnqueueFail 入延时队列失败的自我保护关闭。
	ClosedReasonEnqueueFail ClosedReason = "enqueue_fail"
)

// AnomalyKind 表示订单运行中遇到的异常类别，供 OnAnomaly 钩子告警 / 监控使用。
type AnomalyKind string

const (
	// AnomalyAmountMismatch 支付回调的金额与订单登记金额不一致。
	AnomalyAmountMismatch AnomalyKind = "amount_mismatch"
	// AnomalyTradeNoMismatch 同一订单在不同回调中出现不同交易号。
	AnomalyTradeNoMismatch AnomalyKind = "trade_no_mismatch"
	// AnomalyPaidOnClosed 已关闭的订单收到支付成功的回调。
	AnomalyPaidOnClosed AnomalyKind = "paid_on_closed"
	// AnomalyPaidOnCancelled 已取消的订单收到支付成功回调。Engine 会向支付网关
	// QueryOrder 复核；复核确认已支付且金额匹配时，订单仍保持 StatusCancelled，
	// 不恢复、不履约。业务方应监听此异常并进入退款、对账或人工处理流程。
	AnomalyPaidOnCancelled AnomalyKind = "paid_on_cancelled"
	// AnomalyOrderDisappeared CAS 失败后 recheck 发现订单已消失。
	AnomalyOrderDisappeared AnomalyKind = "order_disappeared"
	// AnomalyUnexpectedStatus 订单处于状态机未覆盖的状态。
	AnomalyUnexpectedStatus AnomalyKind = "unexpected_status"
	// AnomalyDeliveryFailed 履约（OnPaid）失败。
	AnomalyDeliveryFailed AnomalyKind = "delivery_failed"
	// AnomalyGatewayQueryFailed 查询支付网关对账接口失败。
	AnomalyGatewayQueryFailed AnomalyKind = "gateway_query_failed"
	// AnomalyDelayQueueCleanupFailed 订单已支付后清理延时关单队列残留失败；
	// 订单状态正确，但延时队列里残留的订单号会被 CloseWorker 二次拉取（CloseWorker
	// 对 Paid 订单做幂等 skip，正确性不受影响），仍可能产生多余的 close 路径事件 / 日志，
	// 误导监控判断。需要运维感知 Redis / Queue 子系统的可用性。
	AnomalyDelayQueueCleanupFailed AnomalyKind = "delay_queue_cleanup_failed"
	// AnomalyAppendLogFailed 订单流水（LogEntry）写入失败。
	// 主流程已经完成状态推进，仅审计/合规链路出现空洞——典型场景：Store.AppendLog 表
	// 满 / 磁盘满 / 唯一约束冲突。Engine 不阻断主流程（避免审计写入抖动连带订单失败），
	// 但通过 Observer.Event(EventAnomaly) 让监控感知；合规要求严的业务方应配告警。
	AnomalyAppendLogFailed AnomalyKind = "append_log_failed"
	// AnomalySupersededGatewayCloseFailed 订单被 superseded 时本地 CAS Close 成功但网关
	// CloseOrder 失败（仅在 SupersededDegraded 模式下出现）。业务方应通过 hook 或本
	// anomaly 监听做补救——可能存在"用户用旧 prepay_id 仍能在网关侧支付"的窗口。
	AnomalySupersededGatewayCloseFailed AnomalyKind = "superseded_gateway_close_failed"
	// AnomalyRefundGatewayFailed 业务侧退款编排中检测到 Refund / QueryRefund 多次重试
	// 仍失败的非可忽略错误。核心包不参与退款编排——本常量仅供业务方调 Observer.Event 时
	// 使用，让跨项目监控可统一识别。
	AnomalyRefundGatewayFailed AnomalyKind = "refund_gateway_failed"
	// AnomalyRefundDrift 退款异步通知与 QueryRefund 状态不一致（典型场景：通知声称
	// succeeded 但 query 返回 failed），需人工对账。
	AnomalyRefundDrift AnomalyKind = "refund_drift"
	// AnomalyMalformedPaidNotify Paid 状态的网关回调缺失关键字段（TransactionID 为空
	// 或 TotalAmount <= 0）。Engine 会拒绝该回调并返回 nil 给网关，**订单仍处于 Pending**。
	//
	// 恢复路径（不是 QueryOrder 兜底——核心包当前未在此场景主动查网关）：
	//
	//  1. 订单到期后被 CloseFallback / 延时队列推进到 StatusClosed；
	//  2. 网关后续若再发出一份带完整字段的合法 Paid 通知，HandleNotify 会进入
	//     StatusClosed 分支（handleClosedPaidNotify），经 QueryOrder 验证后通过
	//     CASReopenPaid 把订单恢复到 Paid。
	//
	// 若网关此后不再下发合法通知，订单将停留在 Closed——业务方应通过运维告警
	// 介入对账。合法渠道（微信 / 支付宝 / Stripe 等）的 Paid 通知必带这两字段，
	// 触发此异常多半意味着上游签名校验缺陷、伪造请求或 driver bug。
	//
	// 触发点上 order 尚未加载，**不会调用 OnAnomaly 钩子**——仅打 ALERT 日志 +
	// Observer.Event(EventAnomaly)。
	AnomalyMalformedPaidNotify AnomalyKind = "malformed_paid_notify"
)
