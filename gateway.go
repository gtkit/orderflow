package orderflow

import (
	"context"
	"net/http"
	"time"
)

// Channel 表示支付渠道标识（如 "wechat" / "alipay" / "stripe"）。
// 核心包不限定合法值，由 driver 和业务方约定。
type Channel string

// TradeStatus 是对各家支付网关交易状态的统一抽象。
type TradeStatus string

const (
	TradeStatusPaid     TradeStatus = "paid"
	TradeStatusUnpaid   TradeStatus = "unpaid"
	TradeStatusClosed   TradeStatus = "closed"
	TradeStatusRefunded TradeStatus = "refunded"
)

// PaymentGateway 抽象了下单、关闭、查询、回调解析等支付操作。
//
// driver 负责屏蔽各渠道（微信 / 支付宝 / ……）的协议差异，对上层提供统一语义。
//
// # 安全契约（实现方必须遵守）
//
// **ParseNotify 必须完成签名验证后才能返回成功的 NotifyResult**。Engine 的 HandleNotify
// 流程信任 ParseNotify 的输出已经是经过验签的真实网关回调——不做二次验签。如果 driver
// 忽略验签（例如直接 json.Unmarshal HTTP body），攻击者可以伪造 `{"trade_status":"paid",
// "total_amount":1,...}` 的 POST 请求让 Engine 把订单推进到 Paid 并触发 OnPaid 发放权益。
// 这是关键的"默认安全"防线，实现方跳过即视为破坏合约。
//
// 验签失败时 ParseNotify 必须返回 error（**不是** 返回 TradeStatusUnpaid 的 NotifyResult）。
//
// **UnifiedOrder / CloseOrder / QueryOrder** 的实现应自身做网关通信的 TLS + 凭证管理。
type PaymentGateway interface {
	// UnifiedOrder 在支付网关侧创建交易，返回客户端拉起支付所需参数。
	UnifiedOrder(ctx context.Context, ch Channel, req UnifiedOrderRequest) (UnifiedOrderResponse, error)

	// CloseOrder 在支付网关侧关闭交易。Engine 会对"订单不存在 / 已关闭"类错误做容忍，
	// 具体由 IsIgnorableCloseError 判定。
	CloseOrder(ctx context.Context, ch Channel, orderNo string) error

	// QueryOrder 向支付网关查询订单真实状态，用于对账和恢复。
	QueryOrder(ctx context.Context, ch Channel, orderNo string) (QueryResult, error)

	// ParseNotify 解析并验签支付回调请求。
	ParseNotify(ctx context.Context, ch Channel, r *http.Request) (NotifyResult, error)

	// AckNotify 向支付网关回写成功响应。
	AckNotify(ch Channel, w http.ResponseWriter) error

	// IsIgnorableCloseError 判断 CloseOrder 返回的错误是否可安全忽略
	// （典型场景：网关侧订单尚未创建 / 已处于关闭态）。
	IsIgnorableCloseError(ch Channel, err error) bool
}

// UnifiedOrderRequest 是创建支付交易的通用入参。
type UnifiedOrderRequest struct {
	OutTradeNo  string
	TotalAmount int64
	Subject     string
	NotifyURL   string
	ExpireAt    time.Time
	Metadata    map[string]string
}

// UnifiedOrderResponse 是创建支付交易的通用出参。
type UnifiedOrderResponse struct {
	// AppParams 是客户端（APP / 小程序）拉起支付所需的字符串载荷。
	AppParams string
	// Raw 承载 driver 原始响应，供调试或扩展使用。
	Raw any
}

// NotifyResult 是支付回调经验签后抽取出的通用字段集。
type NotifyResult struct {
	OutTradeNo    string
	TransactionID string
	TradeStatus   TradeStatus
	TotalAmount   int64
	PaidAt        time.Time
	Channel       Channel
	Raw           any
}

// QueryResult 是对账查询的通用返回。
type QueryResult struct {
	OutTradeNo    string
	TransactionID string
	TradeStatus   TradeStatus
	TotalAmount   int64
	PaidAt        time.Time
	Channel       Channel
}
