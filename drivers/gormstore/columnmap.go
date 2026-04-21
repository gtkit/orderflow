package gormstore

import (
	"fmt"
	"regexp"
)

// ColumnMap 描述订单表的关键列名。
// 全字段可选——零值会被替换为默认值；非标命名的业务只需填写需要覆盖的列。
//
// 默认值对应 sleep_client 主流命名：order_no / order_token / user_id / status / trade_no ...
//
// **安全约束**：非零值字段必须匹配 `^[a-zA-Z_][a-zA-Z0-9_]*$`，即标准 SQL 标识符。
// 防御场景：如果 ColumnMap 来自外部配置（yaml / json / env）且未经校验，恶意配置
// 可能通过"列名"注入 SQL（例如 "status = 1 OR 1=1 --"）。gormstore 把列名直接拼进
// WHERE / SET 子句，参数化只保护 VALUE 不保护 IDENTIFIER，必须由本层兜底校验。
// 校验失败时 Store.New 返回 error，阻止启动。
type ColumnMap struct {
	OrderNo     string
	OrderToken  string
	UserID      string
	ProductID   string
	Status      string
	TradeNo     string
	PaidAt      string
	DeliveredAt string
	ExpireAt    string
	UpdatedAt   string
}

// SQLIdentifierPattern 是合法 SQL 标识符的正则，公开给 Config.validate 校验表名。
var SQLIdentifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

var columnNamePattern = SQLIdentifierPattern

// validate 校验所有非零字段都是合法 SQL 标识符。调用方应先 validate 再 withDefaults。
func (c ColumnMap) validate() error {
	fields := []struct {
		name string
		val  string
	}{
		{"OrderNo", c.OrderNo},
		{"OrderToken", c.OrderToken},
		{"UserID", c.UserID},
		{"ProductID", c.ProductID},
		{"Status", c.Status},
		{"TradeNo", c.TradeNo},
		{"PaidAt", c.PaidAt},
		{"DeliveredAt", c.DeliveredAt},
		{"ExpireAt", c.ExpireAt},
		{"UpdatedAt", c.UpdatedAt},
	}
	for _, f := range fields {
		if f.val == "" {
			continue // 走默认值
		}
		if !columnNamePattern.MatchString(f.val) {
			return fmt.Errorf("gormstore: ColumnMap.%s %q is not a valid SQL identifier (must match [a-zA-Z_][a-zA-Z0-9_]*)", f.name, f.val)
		}
	}
	return nil
}

func (c ColumnMap) withDefaults() ColumnMap {
	if c.OrderNo == "" {
		c.OrderNo = "order_no"
	}
	if c.OrderToken == "" {
		c.OrderToken = "order_token"
	}
	if c.UserID == "" {
		c.UserID = "user_id"
	}
	if c.ProductID == "" {
		c.ProductID = "product_id"
	}
	if c.Status == "" {
		c.Status = "status"
	}
	if c.TradeNo == "" {
		c.TradeNo = "trade_no"
	}
	if c.PaidAt == "" {
		c.PaidAt = "paid_at"
	}
	if c.DeliveredAt == "" {
		c.DeliveredAt = "delivered_at"
	}
	if c.ExpireAt == "" {
		c.ExpireAt = "expire_at"
	}
	if c.UpdatedAt == "" {
		c.UpdatedAt = "updated_at"
	}
	return c
}
