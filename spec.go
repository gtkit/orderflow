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
	ProductType   string
	ProductTitle  string
	OriginalPrice int64
	DiscountPrice int64
	PayAmount     int64
	PayMethod     string
	ChannelID     int64
	ExpireAt      time.Time
	ClientIP      string
	// Extra 用于持久化业务私有字段（如会员权益快照）。
	// driver 需要自行决定如何映射（独立字段 / JSON 列 / 忽略）。
	Extra map[string]any
}

// ProductInfo 由调用方在 Create 前自行查询并填入 CreateRequest。
// Engine 不关心商品来源，只读取其中的价格、标题等做比价和快照。
type ProductInfo struct {
	ID    uint64
	Type  string
	Title string
	// Price 单位：分（或业务方统一使用的最小货币单位）。
	Price int64
	Extra map[string]any
}

// BillSpec 是履约阶段（Store.FinalizePaidOrder）写入账单表的中性载荷。
type BillSpec struct {
	UserID         int64
	OrderNo        string
	TradeNo        string
	ProductID      uint64
	ProductType    string
	ProductTitle   string
	OriginalPrice  int64
	DiscountAmount int64
	PayAmount      int64
	PayMethod      string
	PayChannel     string
	ChannelID      int64
	PaidAt         time.Time
}
