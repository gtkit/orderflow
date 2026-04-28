package gormstore

import (
	"time"

	"github.com/gtkit/orderflow"
)

// OrderBill 是 gormstore 内置的账单模型。
// 用户通过 Config.BillTable 指定表名；列定义按主流命名约定给出，必要时业务侧可建对应表结构。
type OrderBill struct {
	ID             uint64                `gorm:"column:id;primaryKey;autoIncrement"`
	UserID         int64                 `gorm:"column:user_id;not null;index"`
	OrderNo        string                `gorm:"column:order_no;type:varchar(64);not null;uniqueIndex"`
	TradeNo        string                `gorm:"column:trade_no;type:varchar(128);not null;default:''"`
	ProductID      uint64                `gorm:"column:product_id;not null"`
	ProductType    orderflow.ProductType `gorm:"column:product_type;type:tinyint;not null;default:0"`
	ProductTitle   string                `gorm:"column:product_title;type:varchar(255);not null"`
	OriginalPrice  int64                 `gorm:"column:original_price;not null"`
	DiscountAmount int64                 `gorm:"column:discount_amount;not null;default:0"`
	PayAmount      int64                 `gorm:"column:pay_amount;not null"`
	PayMethod      orderflow.PayMethod   `gorm:"column:pay_method;type:tinyint;not null;default:0"`
	PayChannel     string                `gorm:"column:pay_channel;type:varchar(32);not null;default:''"`
	ChannelID      int64                 `gorm:"column:channel_id;not null;default:0"`
	PaidAt         time.Time             `gorm:"column:paid_at;type:datetime;not null"`
	CreatedAt      time.Time             `gorm:"column:created_at;not null"`
}

// OrderLog 是 gormstore 内置的状态流水模型。
type OrderLog struct {
	ID         uint64                `gorm:"column:id;primaryKey;autoIncrement"`
	OrderNo    string                `gorm:"column:order_no;type:varchar(64);not null;index"`
	UserID     int64                 `gorm:"column:user_id;not null;index"`
	FromStatus orderflow.OrderStatus `gorm:"column:from_status;type:tinyint;not null"`
	ToStatus   orderflow.OrderStatus `gorm:"column:to_status;type:tinyint;not null"`
	Actor      string                `gorm:"column:actor;type:varchar(64);not null;default:''"`
	Remark     string                `gorm:"column:remark;type:varchar(512);not null;default:''"`
	CreatedAt  time.Time             `gorm:"column:created_at;not null"`
}

// buildBill 把 orderflow.BillSpec 转成内置 OrderBill 模型。
func buildBill(spec orderflow.BillSpec) *OrderBill {
	return &OrderBill{
		UserID:         spec.UserID,
		OrderNo:        spec.OrderNo,
		TradeNo:        spec.TradeNo,
		ProductID:      spec.ProductID,
		ProductType:    spec.ProductType,
		ProductTitle:   spec.ProductTitle,
		OriginalPrice:  spec.OriginalPrice,
		DiscountAmount: spec.DiscountAmount,
		PayAmount:      spec.PayAmount,
		PayMethod:      spec.PayMethod,
		PayChannel:     spec.PayChannel,
		ChannelID:      spec.ChannelID,
		PaidAt:         spec.PaidAt,
		CreatedAt:      time.Now(),
	}
}

// buildLog 把 orderflow.LogEntry 转成内置 OrderLog 模型。
func buildLog(entry orderflow.LogEntry) *OrderLog {
	created := entry.CreatedAt
	if created.IsZero() {
		created = time.Now()
	}
	return &OrderLog{
		OrderNo:    entry.OrderNo,
		UserID:     entry.UserID,
		FromStatus: entry.FromStatus,
		ToStatus:   entry.ToStatus,
		Actor:      entry.Actor,
		Remark:     entry.Remark,
		CreatedAt:  created,
	}
}

// wrapLog 反向把 OrderLog 模型还原成 LogEntry。
func wrapLog(m *OrderLog) orderflow.LogEntry {
	return orderflow.LogEntry{
		OrderNo:    m.OrderNo,
		UserID:     m.UserID,
		FromStatus: m.FromStatus,
		ToStatus:   m.ToStatus,
		Actor:      m.Actor,
		Remark:     m.Remark,
		CreatedAt:  m.CreatedAt,
	}
}
