package orderflow

import "time"

// OrderSpec 是 Engine 构造订单时交给 Store 的中性载荷。
// driver 层负责把它映射并写入业务方的订单表。
type OrderSpec struct {
	OrderNo       string
	OrderToken    string
	UserID        int64
	Status        OrderStatus
	ProductID     uint64
	ProductType   ProductType
	ProductTitle  string
	OriginalPrice int64
	DiscountPrice int64
	PayAmount     int64
	PayMethod     PayMethod
	// ChannelID 业务自定义的渠道维度，由 CreateRequest.ChannelID 透传。
	// Engine 不解读；driver 负责持久化（零值 = 不指定渠道）。
	ChannelID int64
	ExpireAt  time.Time
	ClientIP  string
	// Extra 用于持久化业务私有字段（如会员权益快照）。
	// driver 需要自行决定如何映射（独立字段 / JSON 列 / 忽略）。
	Extra map[string]any
}

// ProductInfo 由调用方在 Create 前自行查询并填入 CreateRequest。
// Engine 不关心商品来源，只读取其中的价格、标题等做比价和快照。
type ProductInfo struct {
	ID    uint64
	Type  ProductType
	Title string
	// Price 单位：分（或业务方统一使用的最小货币单位）。
	//
	// 必须 > 0：Engine.Create 在入口强制校验，Price ≤ 0 直接返回 ErrInvalidConfig，
	// 不会落库 / 入队 / 写缓存。底层 paymgr SDK 的 OrderRequest.Validate 也强制
	// total_amount > 0，这里提前拒绝是为了避免无效副作用 + 错误归属混乱。
	//
	// 0 元订单（赠品 / 试用 / 会员体验等）应在业务侧 short-circuit——直接发货 / 发券 /
	// 入会员表，不走 Engine.Create 这条"用户付钱给订单"的支付链路。
	Price int64
	Extra map[string]any
}

// BillSpec 是履约阶段（Store.FinalizePaidOrder）写入账单表的中性载荷。
//
// 字段填充说明：
//   - UserID / OrderNo / ProductID 等均来自 order 快照；
//   - TradeNo / PaidAt 来自支付回调（NotifyResult）；
//   - PayChannel 是网关 driver 返回的渠道代号字符串（如 "wxpay" / "alipay"）；
//   - ChannelID 是业务自定义的渠道维度。OrderSnapshot 接口未暴露 ChannelID()
//     方法（避免强制下游改实现），Engine 当前不填充此字段。driver 若需在账单
//     中回写 channel_id，应在 FinalizePaidOrder 内从订单表自行查询并补写。
type BillSpec struct {
	UserID         int64
	OrderNo        string
	TradeNo        string
	ProductID      uint64
	ProductType    ProductType
	ProductTitle   string
	OriginalPrice  int64
	DiscountAmount int64
	PayAmount      int64
	PayMethod      PayMethod
	PayChannel     string
	ChannelID      int64
	PaidAt         time.Time
}
