package gormstore

import (
	"context"
	"fmt"
	"time"

	"github.com/gtkit/orderflow"
)

// Create 插入订单记录，返回包装后的视图。
func (s *Store[O, M]) Create(ctx context.Context, spec orderflow.OrderSpec) (O, error) {
	var zero O
	m := s.buildModel(spec)
	if err := s.db.WithContext(ctx).Table(s.orderTable).Create(m).Error; err != nil {
		return zero, fmt.Errorf("gormstore: create order: %w", err)
	}
	return s.wrap(m), nil
}

// UpdateByOrderNo 按订单号更新字段。
func (s *Store[O, M]) UpdateByOrderNo(ctx context.Context, orderNo string, updates map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	err := s.db.WithContext(ctx).Table(s.orderTable).
		Where(s.cols.OrderNo+" = ?", orderNo).
		Updates(updates).Error
	if err != nil {
		return fmt.Errorf("gormstore: update by order_no: %w", err)
	}
	return nil
}

// CASClose 把 Pending 订单原子推进到 Closed。
func (s *Store[O, M]) CASClose(ctx context.Context, orderNo string) (int64, error) {
	result := s.db.WithContext(ctx).Table(s.orderTable).
		Where(s.cols.OrderNo+" = ? AND "+s.cols.Status+" = ?", orderNo, orderflow.StatusPending).
		Updates(map[string]any{
			s.cols.Status:    orderflow.StatusClosed,
			s.cols.UpdatedAt: time.Now(),
		})
	if result.Error != nil {
		return 0, fmt.Errorf("gormstore: cas close: %w", result.Error)
	}
	return result.RowsAffected, nil
}

// CASConfirmPaid 把 Pending 订单原子推进到 Paid，并写入 trade_no / paid_at。
func (s *Store[O, M]) CASConfirmPaid(ctx context.Context, orderNo, tradeNo string, paidAt time.Time) (int64, error) {
	result := s.db.WithContext(ctx).Table(s.orderTable).
		Where(s.cols.OrderNo+" = ? AND "+s.cols.Status+" = ?", orderNo, orderflow.StatusPending).
		Updates(map[string]any{
			s.cols.Status:    orderflow.StatusPaid,
			s.cols.TradeNo:   tradeNo,
			s.cols.PaidAt:    paidAt,
			s.cols.UpdatedAt: time.Now(),
		})
	if result.Error != nil {
		return 0, fmt.Errorf("gormstore: cas confirm paid: %w", result.Error)
	}
	return result.RowsAffected, nil
}

// CASReopenPaid 把 Closed 订单恢复为 Paid。用于"本地已关闭但网关确认已扣款"的竞态恢复。
func (s *Store[O, M]) CASReopenPaid(ctx context.Context, orderNo, tradeNo string, paidAt time.Time) (int64, error) {
	result := s.db.WithContext(ctx).Table(s.orderTable).
		Where(s.cols.OrderNo+" = ? AND "+s.cols.Status+" = ?", orderNo, orderflow.StatusClosed).
		Updates(map[string]any{
			s.cols.Status:    orderflow.StatusPaid,
			s.cols.TradeNo:   tradeNo,
			s.cols.PaidAt:    paidAt,
			s.cols.UpdatedAt: time.Now(),
		})
	if result.Error != nil {
		return 0, fmt.Errorf("gormstore: cas reopen paid: %w", result.Error)
	}
	return result.RowsAffected, nil
}
