package orderflow

import "time"

// RefundTradeStatus 是对各家支付网关退款状态的统一抽象。
//
// 与 TradeStatus（支付侧）独立命名空间，避免语义混淆——后者描述订单在
// 网关侧的支付状态，本类型描述退款单在网关侧的处理状态。
//
// 取值约定（业务方应严格按 IsTerminal() 区分中间态与终态）：
//
//   - RefundTradeStatusPending：已提交渠道，渠道侧尚未处理（少见，部分渠道有受理排队）。
//     非终态，等异步通知或主动 Query 推进。
//   - RefundTradeStatusProcessing：渠道侧已受理，处理中（含微信"退款处理中"、支付宝"REFUND_PROCESSING"）。
//     非终态，等异步通知或主动 Query 推进。
//   - RefundTradeStatusSucceeded：终态，款项已原路返回（含微信 SUCCESS、支付宝 REFUND_SUCCESS）。
//     业务方应据此触发反向核销。
//   - RefundTradeStatusFailed：终态，退款明确失败（含渠道返回的 closed / error 等终态失败）。
//     业务方应通知客服，不触发反向核销。
//   - RefundTradeStatusUnknown：状态待定——含两类语义：
//     (1) 渠道返回了 driver 暂未识别的状态字面量（新增渠道行为或 SDK 升级）；
//     (2) 渠道返回的状态需要人工介入才能推进（如微信 ABNORMAL）。
//     业务方应**告警**让人工感知，不触发反向核销，并继续观察 Query 结果——这是非终态。
//
// driver 实现方负责把渠道原始状态映射到这五个值；调用方需要拿到原始状态时
// 通过 RefundQueryResult.Raw / RefundNotifyResult.Raw 类型断言取回 SDK 原始结构。
type RefundTradeStatus string

const (
	// RefundTradeStatusPending 已提交渠道，渠道侧尚未处理（非终态）。
	RefundTradeStatusPending RefundTradeStatus = "pending"
	// RefundTradeStatusProcessing 渠道侧已受理，处理中（非终态）。
	RefundTradeStatusProcessing RefundTradeStatus = "processing"
	// RefundTradeStatusSucceeded 退款成功，款项已原路返回（终态）。
	RefundTradeStatusSucceeded RefundTradeStatus = "succeeded"
	// RefundTradeStatusFailed 退款明确失败（终态）。
	RefundTradeStatusFailed RefundTradeStatus = "failed"
	// RefundTradeStatusUnknown 状态待定（非终态）：渠道返回的状态需人工介入或 driver 暂未识别。
	// 业务方应**告警**让人工感知，不触发反向核销，并继续观察 Query 结果。
	RefundTradeStatusUnknown RefundTradeStatus = "unknown"
)

// String 返回状态的语义名称，用于日志 / 调试。未声明的值返回 "invalid"，不会 panic。
//
// 注意：RefundTradeStatusUnknown.String() 返回 "unknown"——这是合法的状态值，
// 与"未声明的零值 / 非法值"返回的 "invalid" 区分开。
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
	case RefundTradeStatusUnknown:
		return "unknown"
	default:
		return "invalid"
	}
}

// IsTerminal 报告退款是否处于终态（不会再变化）。
//
// 终态：Succeeded / Failed —— 业务方可据此推进本地状态机。
// 非终态：Pending / Processing / Unknown —— 业务方应继续观察。
//
// 特别强调 Unknown 是**非终态**：业务方看到 Unknown 时不应触发反向核销，
// 应记录告警 + 继续走 QueryRefund 路径或等异步通知推进到真正的终态。
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
	// OutTradeNo 原支付订单的商户订单号。
	// OutTradeNo 与 TransactionID 至少填一个；典型场景填 OutTradeNo 即可。
	OutTradeNo string

	// TransactionID 渠道侧交易号（如微信 transaction_id、支付宝 trade_no）。
	// OutTradeNo 与 TransactionID 至少填一个；当业务侧只有 TransactionID 时使用本字段。
	TransactionID string

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

// RefundResponse 是发起退款的同步响应。
//
// **关键字段是 Status**——业务方应据此判断本次同步调用是否已经达到终态。
//
// driver 实装方按"已知渠道行为模式"启发式填写：
//   - 支付宝：同步成功通常即终态 succeeded（渠道侧立即完成退款）→ driver 填 Succeeded
//   - 微信：同步成功通常是 processing（渠道侧后续异步处理）→ driver 填 Processing
//   - 其他渠道：保守填 Processing 让业务方等异步通知 / 主动 Query
//
// **重要：`Status` 是启发式默认值，不是确定性映射**。底层 `paymgr.RefundResponse`
// 当前不暴露原始 status 字段，driver 无法读取真实状态——只能按渠道历史行为约定填写。
// 渠道行为如果发生变化（如支付宝增加风控审查导致同步成功 = processing），driver 默认值
// 会与真实状态偏差。业务方应该：
//
//  1. 用 `Status.IsTerminal()` 判断而**不是**按渠道名硬编码——这样未来扩展或行为变化时
//     业务代码无需修改即保持正确性
//  2. 对状态准确性敏感的场景（如金额较大、审计严格），调用 Refund 后应主动 QueryRefund
//     兜底拉真实状态再触发反向核销，不要完全信任 Status 字段
//
// 业务方收到 Status==Succeeded 时**可以**立即触发反向核销；收到 Processing / Pending /
// Unknown 时**必须**等待异步通知或主动 Query 推进——把渠道异步通知当成事实真源。
type RefundResponse struct {
	// OutRefundNo 商户退款单号，与请求一致。
	OutRefundNo string

	// GatewayRefundID 渠道侧退款单号（如有）。部分渠道在异步通知时才返回，
	// 同步响应里可能为空字符串。
	GatewayRefundID string

	// Status 渠道同步返回的退款状态。driver 按渠道行为契约填写：
	// alipay 同步成功填 Succeeded；wechat 同步成功填 Processing（等异步回调）。
	// 业务方据此判断是否要等异步通知。
	Status RefundTradeStatus

	// RefundAmount 本次退款金额（分）。渠道实际处理的金额，理论上等于请求 RefundAmount，
	// 但部分渠道（如支付宝按金额单位换算）可能有 1 分以内偏差——以本字段为准。
	RefundAmount int64

	// Channel 支付渠道。冗余字段方便业务方一站式拿到完整上下文。
	Channel Channel

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
	// OutTradeNo 原支付订单号。便于业务方做多表关联。
	OutTradeNo string
	// TransactionID 渠道侧交易号。
	TransactionID string
	// GatewayRefundID 渠道侧退款单号。
	GatewayRefundID string
	// Status 退款在网关侧的当前状态。
	Status RefundTradeStatus
	// RefundAmount 本次退款金额，单位：分。
	RefundAmount int64
	// TotalAmount 原订单总金额，单位：分。便于业务方对账。
	TotalAmount int64
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
// 字段集合与 RefundQueryResult 基本一致——异步通知和主动查询本质都是
// 拿渠道侧的退款状态，调用方可按相同结构处理。
//
// 实现方必须遵守的安全契约见 RefundGateway.ParseRefundNotify GoDoc。
type RefundNotifyResult struct {
	// OutRefundNo 商户退款单号。
	OutRefundNo string
	// OutTradeNo 原支付订单号。
	OutTradeNo string
	// TransactionID 渠道侧交易号。
	TransactionID string
	// GatewayRefundID 渠道侧退款单号。
	GatewayRefundID string
	// Status 退款状态。
	Status RefundTradeStatus
	// RefundAmount 本次退款金额，单位：分。
	RefundAmount int64
	// TotalAmount 原订单总金额，单位：分。
	TotalAmount int64
	// SucceededAt 退款成功时间。仅当 Status == RefundTradeStatusSucceeded 时有效。
	SucceededAt time.Time
	// Channel 支付渠道。
	Channel Channel
	// UserReceivedAccount 退款入账方（仅微信返回，如 "招商银行信用卡0403"）。
	// 其他渠道为空字符串。供客服查询使用。
	UserReceivedAccount string
	// Raw 渠道原始响应（含完整 SDK 字段），供调试 / 扩展使用。
	Raw any
}
