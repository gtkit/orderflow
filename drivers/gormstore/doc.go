// Package gormstore 用 GORM 实现 orderflow.Store。
//
// 设计取舍：
//
//   - 用户带自己的 Order GORM 模型 M，通过 Wrap / BuildModel 两个适配函数注入；
//   - gormstore 自带 OrderBill / OrderLog 两个标准模型，用户只需指定表名；
//   - 订单表的列名通过 ColumnMap 定制，默认值贴合主流命名（order_no / status / trade_no ...）；
//   - 状态列默认存 orderflow.OrderStatus 的规范整数值
//     （0=Pending / 10=Paid / 20=Delivered / 30=Completed / 40=Closed / 50=Cancelled）；
//     业务已有其它编码时需要做一次数据迁移。
//   - 支付方式列与商品类型列默认存 typed enum 数值（tinyint）：
//     PayMethod (1=微信 / 2=支付宝 / 3=银联)；ProductType (1=文本 / 2=视频 / 3=音频 / 99=会员)。
//
// # 必备索引清单
//
// 业务方需要在订单表上建以下索引，否则 fallback worker 与查询路径会全表扫描：
//
//	-- 唯一约束（建议）
//	UNIQUE INDEX uk_order_no    ON orders (order_no);
//	UNIQUE INDEX uk_order_token ON orders (order_token);
//
//	-- fallback 扫描路径
//	INDEX idx_status_expire_at  ON orders (status, expire_at);  -- FindExpiredPending
//	INDEX idx_status_paid_at    ON orders (status, paid_at);    -- FindPaidUndelivered
//
//	-- 复用查询路径
//	INDEX idx_user_product_status ON orders (user_id, product_id, status);  -- FindPendingByUserAndProduct
//	-- 强烈建议加部分唯一索引（PostgreSQL）作为"一用户一商品一 Pending"兜底：
//	-- CREATE UNIQUE INDEX uk_user_product_pending ON orders (user_id, product_id) WHERE status = 0;
//	-- MySQL 可用生成列 + 普通唯一索引模拟。
//
//	-- 用户列表
//	INDEX idx_user_id ON orders (user_id);
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
