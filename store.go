package orderflow

import (
	"context"
	"time"
)

// Store 承载订单与账单的持久化能力。
//
// 所有 CAS 方法返回 affected 行数：0 表示并发已经抢先推进了状态，调用方需要 recheck
// 并根据当前状态决定后续动作。非 0 表示当次 CAS 成功。
type Store[O OrderSnapshot] interface {
	// ----- 读路径 -----

	GetByNo(ctx context.Context, orderNo string, fields ...string) (O, bool, error)
	GetByToken(ctx context.Context, orderToken string) (O, bool, error)
	ListByUser(ctx context.Context, userID int64, fields ...string) ([]O, error)
	FindPendingByUserAndProduct(ctx context.Context, userID int64, productID uint64) (O, bool, error)
	FindExpiredPending(ctx context.Context, limit int) ([]string, error)
	FindPaidUndelivered(ctx context.Context, limit int) ([]string, error)

	// ----- 写路径 -----

	Create(ctx context.Context, spec OrderSpec) (O, error)
	// UpdateByOrderNo 按订单号更新指定字段。
	// orderNo 对应订单不存在时必须返回 ErrOrderNotFound（errors.Is 可检测），
	// 避免无声失败掩盖业务 bug。
	UpdateByOrderNo(ctx context.Context, orderNo string, updates map[string]any) error

	// ----- 状态跃迁（CAS，幂等） -----

	// CASClose 将 Pending 订单原子推进到 Closed。
	CASClose(ctx context.Context, orderNo string) (int64, error)
	// CASCancel 将 Pending 订单原子推进到 Cancelled（用户主动取消语义）。
	// 与 CASClose 区别：终态值不同（StatusCancelled vs StatusClosed），
	// 业务侧通过状态值区分"用户主动放弃"与"系统超时关闭/管理员关闭"。
	CASCancel(ctx context.Context, orderNo string) (int64, error)
	// CASConfirmPaid 将 Pending 订单原子推进到 Paid，并写入交易号和支付时间。
	//
	// expectedAmount 是订单登记的应付金额（来自 OrderSnapshot.PayAmount()），
	// driver 必须把它加入 CAS WHERE 子句作为二级金额校验——只有 DB 当前
	// pay_amount 列值与 expectedAmount 严格相等时才推进状态。这保证了即使上游
	// 的 amount-mismatch 校验被绕过、或 DB 中的 pay_amount 列被外部修改，错金额
	// 的支付回调也无法把订单推进到 Paid。
	CASConfirmPaid(ctx context.Context, orderNo, tradeNo string, paidAt time.Time, expectedAmount int64) (int64, error)
	// CASReopenPaid 将已 Closed 的订单恢复到 Paid（适用于"本地已关闭但支付网关已扣款"的竞态）。
	// expectedAmount 语义同 CASConfirmPaid（强制二级金额校验）。
	CASReopenPaid(ctx context.Context, orderNo, tradeNo string, paidAt time.Time, expectedAmount int64) (int64, error)

	// ----- 履约：订单状态推进到 Delivered 并写入账单，需要在同一事务内完成 -----
	//
	// driver 如需持久化业务权益快照（VIP 到期时间、积分余额等），应从 order.Extra()
	// 读取并写入业务自有表——在同一事务内完成，保证"订单已 Delivered 但权益未激活"
	// 的状态不会出现。
	FinalizePaidOrder(ctx context.Context, order O, bill BillSpec) error

	// ----- 操作日志（幂等追加） -----

	AppendLog(ctx context.Context, entry LogEntry) error
	ListLogsByOrderNo(ctx context.Context, orderNo string) ([]LogEntry, error)
}

// LogEntry 表示订单状态变更流水的一条记录。
// driver 决定如何映射到日志表，核心包只要求字段语义一致。
type LogEntry struct {
	OrderNo    string
	UserID     int64
	FromStatus OrderStatus
	ToStatus   OrderStatus
	// Actor 记录操作方："system" / "user:<id>" / "admin:<id>" 等约定字符串。
	Actor     string
	Remark    string
	CreatedAt time.Time
}
