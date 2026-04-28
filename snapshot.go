package orderflow

import "time"

// OrderSnapshot 是业务 Order 类型必须暴露的只读视图。
//
// Engine 只通过这个接口读取订单状态，不直接访问结构体字段，这样业务方可以自由
// 设计 Order 的字段名、表结构和嵌入关系。
//
// 约定：
//   - OrderToken 是对外暴露给客户端的不可伪造 token，用作轮询 / 订阅的 key；
//     默认 16 字节 crypto/rand（128bit 熵）。**业务方必须只在订单所有者自己的
//     会话内传递 token**——不要把含 token 的 URL 入分享链接 / 邮件 / 短信。
//     PollStatus / Subscribe 已经做 user_id 归属校验，但流量到达后端前无防护，
//     攻击者拿到 token + user_id 即可订阅状态变更，可能泄露用户支付节奏。
//   - TradeNo 可能为空（尚未支付），空串表示未知；
//   - PaidAt 用 (time.Time, bool) 形式区分"未支付"与"已支付但零值时间戳"；
//   - Extra 用于承载业务私有数据（如会员权益快照），Engine 透传不解读。
type OrderSnapshot interface {
	OrderNo() string
	OrderToken() string
	UserID() int64
	Status() OrderStatus
	ProductID() uint64
	ProductType() ProductType
	ProductTitle() string
	PayMethod() PayMethod
	PayAmount() int64
	OriginalPrice() int64
	TradeNo() string
	ExpireAt() time.Time
	PaidAt() (time.Time, bool)
	Extra() map[string]any
}
