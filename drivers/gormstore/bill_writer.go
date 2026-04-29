package gormstore

import (
	"fmt"

	"github.com/gtkit/orderflow"
	"gorm.io/gorm"
)

// BillWriter 持久化一条账单到下游存储。
//
// gormstore 在 Store.FinalizePaidOrder 的事务内调用 Write，调用方负责传入当前事务的
// *gorm.DB。spec 是已合并 channel_id 回查结果的 BillSpec 副本（详见 ColumnMap.ChannelID
// 文档），实现方可直接当作"待持久化的最终值"使用。
//
// 实现自定义 BillWriter 的典型场景：
//   - 业务方账单表字段与内置 OrderBill 不一致（多列/少列/类型不同）；
//   - 账单写入需要触发额外的业务侧关联表写入；
//   - 账单需要双写（如 DB + ES）。
//
// 实现方应在事务内同步完成所有写操作；返回 error 会让 FinalizePaidOrder 整体回滚。
type BillWriter interface {
	Write(tx *gorm.DB, spec orderflow.BillSpec) error
}

// defaultBillWriter 是 gormstore 内置的 BillWriter 实现，按 OrderBill struct 写入
// Config.BillTable 指定的表。Config.BillWriter 零值时由 New 自动注入。
type defaultBillWriter struct {
	table string
}

func newDefaultBillWriter(table string) *defaultBillWriter {
	return &defaultBillWriter{table: table}
}

// Write 把 BillSpec 转换为 OrderBill 模型并通过 GORM 插入。
//
// 错误信息保持 "gormstore: finalize insert bill: ..." 措辞，以便升级前后日志一致。
func (w *defaultBillWriter) Write(tx *gorm.DB, spec orderflow.BillSpec) error {
	m := buildBill(spec)
	if err := tx.Table(w.table).Create(m).Error; err != nil {
		return fmt.Errorf("gormstore: finalize insert bill: %w", err)
	}
	return nil
}
