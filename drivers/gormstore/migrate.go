package gormstore

import (
	"fmt"

	"gorm.io/gorm"
)

// AutoMigrate 自动建 gormstore 自带的 bill / log 表（如果不存在）。
//
// **不会动业务订单表**：订单表的字段和索引由业务方掌握，gormstore 没有标准
// 模型——本 helper 只覆盖 OrderBill / OrderLog 两个内置模型。
//
// 调用方需在传 GORM 时通过 `db.Table(...)` 或者临时改 `tabler`（GORM v1）
// 把表名改成 Config 里指定的 BillTable / LogTable。这里直接接受表名参数，
// 用 `Set("gorm:table_options", ...)` 等不影响。
//
// 使用示例：
//
//	if err := gormstore.AutoMigrate(db, "order_bills", "order_logs"); err != nil {
//	    log.Fatal(err)
//	}
//
// 业务订单表的索引清单见 doc.go。
func AutoMigrate(db *gorm.DB, billTable, logTable string) error {
	if db == nil {
		return fmt.Errorf("gormstore: AutoMigrate: db must not be nil")
	}
	if billTable == "" || logTable == "" {
		return fmt.Errorf("gormstore: AutoMigrate: billTable and logTable must not be empty")
	}
	if !SQLIdentifierPattern.MatchString(billTable) {
		return fmt.Errorf("gormstore: AutoMigrate: invalid billTable %q", billTable)
	}
	if !SQLIdentifierPattern.MatchString(logTable) {
		return fmt.Errorf("gormstore: AutoMigrate: invalid logTable %q", logTable)
	}
	if err := db.Table(billTable).AutoMigrate(&OrderBill{}); err != nil {
		return fmt.Errorf("gormstore: migrate %s: %w", billTable, err)
	}
	if err := db.Table(logTable).AutoMigrate(&OrderLog{}); err != nil {
		return fmt.Errorf("gormstore: migrate %s: %w", logTable, err)
	}
	return nil
}
