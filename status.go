package orderflow

// OrderStatus 表示订单在生命周期内的状态。
//
// 合法跃迁路径：
//
//	Pending   -> Paid | Closed | Cancelled
//	Paid      -> Delivered | Closed (特殊：Closed 后又支付成功，由 CASReopenPaid 恢复)
//	Delivered -> Completed
//	其余状态为终态，不再跃迁。
type OrderStatus int8

const (
	// StatusUnknown 零值，占位用，语义上不应出现在业务逻辑中。
	StatusUnknown OrderStatus = 0
	// StatusPending 订单已创建，等待支付。
	StatusPending OrderStatus = 1
	// StatusPaid 支付网关已确认收款，等待履约。
	StatusPaid OrderStatus = 2
	// StatusDelivered 权益已发放完成。
	StatusDelivered OrderStatus = 3
	// StatusCompleted 订单生命周期结束（含客户确认、售后窗口关闭等）。
	StatusCompleted OrderStatus = 4
	// StatusClosed 因超时或主动关闭而终结。
	StatusClosed OrderStatus = 5
	// StatusCancelled 用户主动取消。
	StatusCancelled OrderStatus = 6
)

// String 返回状态的语义名称，用于日志 / 调试。
func (s OrderStatus) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusPaid:
		return "paid"
	case StatusDelivered:
		return "delivered"
	case StatusCompleted:
		return "completed"
	case StatusClosed:
		return "closed"
	case StatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// IsTerminal 判断订单是否处于终态（不会再跃迁）。
func (s OrderStatus) IsTerminal() bool {
	switch s {
	case StatusCompleted, StatusClosed, StatusCancelled:
		return true
	default:
		return false
	}
}

// CanTransitionTo 判断从当前状态到目标状态是否是合法跃迁。
// 注意：CASReopenPaid 是对"Closed -> Paid"的例外恢复路径，不走此表。
func (s OrderStatus) CanTransitionTo(next OrderStatus) bool {
	switch s {
	case StatusPending:
		return next == StatusPaid || next == StatusClosed || next == StatusCancelled
	case StatusPaid:
		return next == StatusDelivered || next == StatusClosed
	case StatusDelivered:
		return next == StatusCompleted
	default:
		return false
	}
}
