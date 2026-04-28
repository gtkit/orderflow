package orderflow

import "time"

// DefaultOrderExpire 订单支付有效期的默认值。
const DefaultOrderExpire = 30 * time.Minute

// DefaultCreateLockTTL Engine.Create 的 Locker 持锁时长默认值。
// 10 秒足够覆盖正常的 UnifiedOrder RTT + DB 写入；若经常超时说明网关响应异常或 DB 慢。
const DefaultCreateLockTTL = 10 * time.Second

// CreateRequest 是 Engine.Create 的入参。
// Product 信息由调用方自行查询并填入，Engine 不承担商品校验 / 业务规则。
type CreateRequest struct {
	UserID    int64
	PayMethod PayMethod
	// ChannelID 是业务自定义的渠道维度（如商户子渠道、推广来源等）。
	// Engine 不解读其含义，仅透传到 OrderSpec.ChannelID 交由 Store driver 持久化。
	// 零值表示不指定；业务需要时自行约定编码。
	ChannelID int64
	ClientIP  string
	Product   ProductInfo
}

// CreateResult 是 Engine.Create 的出参。
type CreateResult[O OrderSnapshot] struct {
	Order O
	// PaymentParams 是支付网关返回的拉起支付所需字符串，客户端直接使用。
	PaymentParams string
	// Reused 为 true 表示本次返回的是复用的已存在 Pending 订单。
	Reused bool
}

// StatusResult 是 Engine.PollStatus 的出参。
type StatusResult struct {
	Status     OrderStatus
	StatusText string
}

// Timeline 是订单状态变更时间线，用于客户端详情页展示。
type Timeline struct {
	OrderToken string
	OrderNo    string
	Status     OrderStatus
	Entries    []LogEntry
}

// CloseRequest 触发订单关闭。由 worker / 后台接口使用。
type CloseRequest struct {
	OrderNo string
	Reason  ClosedReason
}
