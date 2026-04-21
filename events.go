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
	// AnomalyOrderDisappeared CAS 失败后 recheck 发现订单已消失。
	AnomalyOrderDisappeared AnomalyKind = "order_disappeared"
	// AnomalyUnexpectedStatus 订单处于状态机未覆盖的状态。
	AnomalyUnexpectedStatus AnomalyKind = "unexpected_status"
	// AnomalyDeliveryFailed 履约（OnPaid）失败。
	AnomalyDeliveryFailed AnomalyKind = "delivery_failed"
	// AnomalyGatewayQueryFailed 查询支付网关对账接口失败。
	AnomalyGatewayQueryFailed AnomalyKind = "gateway_query_failed"
)
