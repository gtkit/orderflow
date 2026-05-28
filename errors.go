package orderflow

import "errors"

// Sentinel 错误。调用方应使用 errors.Is 进行比较，便于封装 / 升级时保持兼容。
var (
	// ErrOrderNotFound 订单不存在（查 token / orderNo 都落空）。
	ErrOrderNotFound = errors.New("orderflow: order not found")
	// ErrOrderForbidden token 合法但访问者 UserID 不匹配。
	ErrOrderForbidden = errors.New("orderflow: order forbidden")
	// ErrOrderExpired 订单已超过支付有效期。
	ErrOrderExpired = errors.New("orderflow: order expired")
	// ErrInvalidTransition 当前状态不允许跃迁到目标状态。
	ErrInvalidTransition = errors.New("orderflow: invalid status transition")
	// ErrPaymentAmountMismatch 支付回调金额与订单金额不一致。
	ErrPaymentAmountMismatch = errors.New("orderflow: payment amount mismatch")
	// ErrOrderAlreadyPaid 表示用户取消订单时支付流程已抢先完成。
	// 调用方应将该错误翻译为"订单已支付，不能取消"，而不是展示取消成功。
	ErrOrderAlreadyPaid = errors.New("orderflow: order already paid")
	// ErrMissingDep 构造 Engine 时必填依赖缺失（返回自 New / 配置校验）。
	ErrMissingDep = errors.New("orderflow: required dependency missing")
	// ErrInvalidConfig 配置字段非法（如 OrderExpire 为负）。
	ErrInvalidConfig = errors.New("orderflow: invalid config")
	// ErrConcurrentCreate 当 Config.Locker 已注入但其他并发 Create 正持有同用户同商品的锁，
	// 本次 Create 放弃。业务方应在 API 层把此错误翻译为 "操作太频繁，请稍后重试" 或等价提示。
	ErrConcurrentCreate = errors.New("orderflow: concurrent create in progress")
	// ErrRefundNotFound RefundGateway.QueryRefund / ParseRefundNotify 在网关侧找不到对应退款单。
	// driver 实装方负责把渠道返回的"退款单不存在"类错误统一包成此 sentinel，调用方据此
	// 决定走"DB 已 pending 但渠道侧从未受理"的兜底路径。
	ErrRefundNotFound = errors.New("orderflow: refund not found")
)
