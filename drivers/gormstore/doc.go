// Package gormstore 用 GORM 实现 orderflow.Store。
//
// 设计取舍：
//
//   - 用户带自己的 Order GORM 模型 M，通过 Wrap / BuildModel 两个适配函数注入；
//   - gormstore 自带 OrderBill / OrderLog 两个标准模型，用户只需指定表名；
//   - 订单表的列名通过 ColumnMap 定制，默认值贴合主流命名（order_no / status / trade_no ...）；
//   - 状态列默认存 orderflow.OrderStatus 的规范整数值（1..6）。业务已有其它编码时需要做一次数据迁移。
//
// 使用示例：
//
//	store, err := gormstore.New[MyView, MyOrder](gormstore.Config[MyView, MyOrder]{
//	    DB:         db,
//	    OrderTable: "orders",
//	    BillTable:  "order_bills",
//	    LogTable:   "order_logs",
//	    Wrap:       func(m *MyOrder) MyView { return MyView{Order: m} },
//	    BuildModel: func(spec orderflow.OrderSpec) *MyOrder { ... },
//	})
//
//	engine, _ := orderflow.New[MyView](orderflow.Config[MyView]{
//	    Store: store,
//	    // ...其它能力接口
//	})
package gormstore
