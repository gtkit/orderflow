package gormstore

import (
	"context"
	"fmt"

	"github.com/gtkit/orderflow"
	"gorm.io/gorm"
)

// LogStore 负责订单状态流水的追加与查询。
//
// gormstore.Store 的 AppendLog / ListLogsByOrderNo 直接委托给本接口，使下游可在不
// 修改 driver 源码的前提下替换流水的存储介质（自有日志表、ES、专用审计服务等）。
//
// db 参数已 WithContext，实现方应继承上下文并在内部决定具体读写策略；本接口未约束
// 事务边界——AppendLog 在 gormstore 设计中是独立写入，不与订单状态跃迁同事务。
type LogStore interface {
	Append(ctx context.Context, db *gorm.DB, entry orderflow.LogEntry) error
	List(ctx context.Context, db *gorm.DB, orderNo string) ([]orderflow.LogEntry, error)
}

// defaultLogStore 是 gormstore 内置的 LogStore 实现，按 OrderLog struct 读写
// Config.LogTable 指定的表。Config.LogStore 零值时由 New 自动注入。
type defaultLogStore struct {
	table string
}

func newDefaultLogStore(table string) *defaultLogStore {
	return &defaultLogStore{table: table}
}

// Append 写入一条流水。错误信息保持 "gormstore: append log: ..." 措辞。
func (s *defaultLogStore) Append(ctx context.Context, db *gorm.DB, entry orderflow.LogEntry) error {
	m := buildLog(entry)
	if err := db.WithContext(ctx).Table(s.table).Create(m).Error; err != nil {
		return fmt.Errorf("gormstore: append log: %w", err)
	}
	return nil
}

// List 按订单号读取流水，按 created_at ASC, id ASC 排序——与升级前完全一致。
func (s *defaultLogStore) List(ctx context.Context, db *gorm.DB, orderNo string) ([]orderflow.LogEntry, error) {
	var ms []*OrderLog
	err := db.WithContext(ctx).Table(s.table).
		Where("order_no = ?", orderNo).
		Order("created_at ASC, id ASC").
		Find(&ms).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: list logs: %w", err)
	}
	entries := make([]orderflow.LogEntry, len(ms))
	for i, m := range ms {
		entries[i] = wrapLog(m)
	}
	return entries, nil
}
