package gormstore

import (
	"context"
	"fmt"

	"github.com/gtkit/orderflow"
)

// AppendLog 追加一条订单状态流水。
func (s *Store[O, M]) AppendLog(ctx context.Context, entry orderflow.LogEntry) error {
	m := buildLog(entry)
	if err := s.db.WithContext(ctx).Table(s.logTable).Create(m).Error; err != nil {
		return fmt.Errorf("gormstore: append log: %w", err)
	}
	return nil
}

// ListLogsByOrderNo 按订单号返回流水，按 created_at 升序。
func (s *Store[O, M]) ListLogsByOrderNo(ctx context.Context, orderNo string) ([]orderflow.LogEntry, error) {
	var ms []*OrderLog
	err := s.db.WithContext(ctx).Table(s.logTable).
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
