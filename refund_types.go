package orderflow

import "time"

// RefundTradeStatus 是对各家支付网关退款状态的统一抽象。
//
// 与 TradeStatus（支付侧）独立命名空间，避免语义混淆——后者描述订单在
// 网关侧的支付状态，本类型描述退款单在网关侧的处理状态。
//
// 取值约定：
//
//   - RefundTradeStatusPending：已提交渠道，渠道侧尚未处理（少见，部分渠道有受理排队）。
//   - RefundTradeStatusProcessing：渠道侧已受理，处理中（含微信"退款处理中"、支付宝"REFUND_PROCESSING"）。
//   - RefundTradeStatusSucceeded：终态，款项已原路返回（含微信 SUCCESS、支付宝 REFUND_SUCCESS）。
//   - RefundTradeStatusFailed：终态，退款失败（含渠道返回的关闭 / 异常 / 未知错误等所有非成功终态）。
//
// driver 实现方负责把渠道原始状态映射到这四个值；调用方需要拿到原始状态时
// 通过 RefundQueryResult.Raw / RefundNotifyResult.Raw 类型断言取回 SDK 原始结构。
type RefundTradeStatus string

const (
	// RefundTradeStatusPending 已提交渠道，渠道侧尚未处理。
	RefundTradeStatusPending RefundTradeStatus = "pending"
	// RefundTradeStatusProcessing 渠道侧已受理，处理中。
	RefundTradeStatusProcessing RefundTradeStatus = "processing"
	// RefundTradeStatusSucceeded 退款成功，款项已原路返回（终态）。
	RefundTradeStatusSucceeded RefundTradeStatus = "succeeded"
	// RefundTradeStatusFailed 退款失败（终态，含关闭 / 异常 / 未知错误）。
	RefundTradeStatusFailed RefundTradeStatus = "failed"
)

// String 返回状态的语义名称，用于日志 / 调试。未定义值返回 "unknown"，不会 panic。
func (s RefundTradeStatus) String() string {
	switch s {
	case RefundTradeStatusPending:
		return "pending"
	case RefundTradeStatusProcessing:
		return "processing"
	case RefundTradeStatusSucceeded:
		return "succeeded"
	case RefundTradeStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// IsTerminal 报告退款是否处于终态（不会再变化）。
// Succeeded / Failed 为 true；Pending / Processing 为 false。
func (s RefundTradeStatus) IsTerminal() bool {
	switch s {
	case RefundTradeStatusSucceeded, RefundTradeStatusFailed:
		return true
	default:
		return false
	}
}

// RefundRequest 是发起退款的通用入参。
//
// driver 实装方负责把本结构映射到底层 SDK 的请求类型；调用方按业务侧审批后的
// 最终金额构造本结构后传给 RefundGateway.Refund。
type RefundRequest struct {
	// OutTradeNo 原支付订单的商户订单号。必填。
	OutTradeNo string

	// OutRefundNo 业务方生成的退款单号，必填，全局唯一，作为业务侧的幂等键。
	//
	// 同一个 OutRefundNo 多次调用 Refund 时，driver 通过 IsIgnorableRefundError
	// 把渠道侧返回的"退款单已存在"映射为可忽略错误，调用方据此走 QueryRefund 路径
	// 拿真实状态。长度 / 字符集需符合各渠道约束（一般 ≤ 64 字符、字母数字下划线）。
	OutRefundNo string

	// RefundAmount 本次退款金额，单位：分。必填，必须 > 0。
	//
	// 重要：本字段应传入**业务侧审批后的最终金额**——若业务允许审批人调整退款金额
	// （扣手续费、按使用比例退、补偿券折抵等），调整由业务方在调用 Refund 之前完成；
	// 本字段不参与审批，仅作为最终发往渠道的金额。
	RefundAmount int64

	// TotalAmount 原订单总金额，单位：分。必填——支付宝退款 API 强制校验，
	// driver 实装即使内部不需要也必须能透传给底层 SDK。
	TotalAmount int64

	// Reason 退款原因，渠道侧会展示给用户。建议 ≤ 80 字。
	Reason string

	// NotifyURL 退款异步通知地址。可选——零值时由 driver 实装按渠道默认值处理。
	NotifyURL string

	// Metadata 附加数据，driver 透传给渠道侧（如微信 attach 字段），渠道在异步通知中原样回传。
	// 业务方可塞业务追踪 ID（审批单 ID、客服 ticket 号等）便于回调时关联。
	Metadata map[string]string
}

// RefundResponse 是发起退款的通用出参。
//
// driver 实装方负责把渠道侧响应映射到本结构；Raw 字段保留渠道原始响应，
// 业务方需要扩展字段时可通过类型断言取回 SDK 类型。
type RefundResponse struct {
	// OutRefundNo 商户退款单号，与请求一致。
	OutRefundNo string

	// GatewayRefundID 渠道侧退款单号（如有）。部分渠道在异步通知时才返回，
	// 同步响应里可能为空字符串。
	GatewayRefundID string

	// Raw 渠道原始响应，供调试 / 扩展使用。具体类型由 driver 决定。
	Raw any
}

// RefundQueryResult 是退款查询的通用返回。
//
// 与 RefundNotifyResult 字段集合完全一致，让调用方在主动 Query 与异步通知
// 两条路径用相同代码处理。
type RefundQueryResult struct {
	// OutRefundNo 商户退款单号。
	OutRefundNo string
	// GatewayRefundID 渠道侧退款单号。
	GatewayRefundID string
	// Status 退款在网关侧的当前状态。
	Status RefundTradeStatus
	// RefundAmount 本次退款金额，单位：分。
	RefundAmount int64
	// SucceededAt 退款成功时间。仅当 Status == RefundTradeStatusSucceeded 时有效，
	// 其他状态下为零值。
	SucceededAt time.Time
	// Channel 支付渠道。
	Channel Channel
	// Raw 渠道原始响应，供调试 / 扩展使用。
	Raw any
}

// RefundNotifyResult 是退款异步通知经验签后抽取出的通用字段集。
//
// 字段集合与 RefundQueryResult 完全一致——异步通知和主动查询本质都是
// 拿渠道侧的退款状态，调用方按相同结构处理。
//
// 实现方必须遵守的安全契约见 RefundGateway.ParseRefundNotify GoDoc。
type RefundNotifyResult struct {
	// OutRefundNo 商户退款单号。
	OutRefundNo string
	// GatewayRefundID 渠道侧退款单号。
	GatewayRefundID string
	// Status 退款状态。
	Status RefundTradeStatus
	// RefundAmount 本次退款金额，单位：分。
	RefundAmount int64
	// SucceededAt 退款成功时间。仅当 Status == RefundTradeStatusSucceeded 时有效。
	SucceededAt time.Time
	// Channel 支付渠道。
	Channel Channel
	// Raw 渠道原始响应（含完整 SDK 字段），供调试 / 扩展使用。
	Raw any
}
