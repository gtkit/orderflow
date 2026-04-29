package gormstore

import (
	"context"

	"github.com/gtkit/orderflow"
)

// AppendLog 追加一条订单状态流水。委托给 Config.LogStore（零值时为内置默认实现）。
func (s *Store[O, M]) AppendLog(ctx context.Context, entry orderflow.LogEntry) error {
	return s.logStore.Append(ctx, s.db, entry)
}

// ListLogsByOrderNo 按订单号返回流水。委托给 Config.LogStore（零值时为内置默认实现）。
func (s *Store[O, M]) ListLogsByOrderNo(ctx context.Context, orderNo string) ([]orderflow.LogEntry, error) {
	return s.logStore.List(ctx, s.db, orderNo)
}
