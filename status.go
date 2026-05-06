package orderflow

// OrderStatus 表示订单在生命周期内的状态。
//
// 数值布局与下游业务参考标准对齐，便于业务方直接持久化为 tinyint 列：
//
//	0  Pending   待支付（零值即此状态）
//	10 Paid      已支付，待履约
//	20 Delivered 权益已发放
//	30 Completed 订单生命周期结束
//	40 Closed    主动关闭 / 系统关闭 / 支付超时
//	50 Cancelled 用户主动取消
//
// 合法跃迁路径：
//
//	Pending   -> Paid | Closed | Cancelled
//	Paid      -> Delivered | Closed (特殊：Closed 后又支付成功，由 CASReopenPaid 恢复)
//	Delivered -> Completed
//	其余状态（Completed / Closed / Cancelled）为终态，不再跃迁。
type OrderStatus int8

const (
	// StatusPending 订单已创建，等待支付（零值）。
	StatusPending OrderStatus = 0
	// StatusPaid 支付网关已确认收款，等待履约。
	StatusPaid OrderStatus = 10
	// StatusDelivered 权益已发放完成。
	StatusDelivered OrderStatus = 20
	// StatusCompleted 订单生命周期结束（含客户确认、售后窗口关闭等）。
	StatusCompleted OrderStatus = 30
	// StatusClosed 因主动关闭、系统异常或支付超时而终结。
	// 关闭原因（含 timeout）通过 ClosedReason 区分，详见 events.go。
	StatusClosed OrderStatus = 40
	// StatusCancelled 用户主动取消（CancelByUser 推进路径，与 StatusClosed 区分）。
	//
	// 与 StatusClosed 的语义边界：
	//
	//   - StatusClosed：系统型终止（支付超时、管理员强制关闭 CloseByAdmin、被新订单
	//     取代关闭、入延时队列失败兜底）。原因通过 ClosedReason 区分。
	//   - StatusCancelled：用户型终止（用户主动放弃支付，调用 CancelByUser）。
	//     原因通过 CancelByUser 入参的 reason 字符串区分（业务自定义文案）。
	//
	// 状态机仅允许 Pending → Cancelled，业务方应通过 Engine.CancelByUser 推进，
	// 不要绕过 Engine 直接写入此状态。
	StatusCancelled OrderStatus = 50
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
// 注意：CASReopenPaid 是对 "Closed -> Paid" 的例外恢复路径，不走此表。
func (s OrderStatus) CanTransitionTo(next OrderStatus) bool {
	switch s {
	case StatusPending:
		return next == StatusPaid ||
			next == StatusClosed ||
			next == StatusCancelled
	case StatusPaid:
		return next == StatusDelivered ||
			next == StatusClosed
	case StatusDelivered:
		return next == StatusCompleted
	default:
		return false
	}
}
