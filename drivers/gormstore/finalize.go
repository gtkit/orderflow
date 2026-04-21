package gormstore

import (
	"context"
	"fmt"
	"time"

	"github.com/gtkit/orderflow"
	"gorm.io/gorm"
)

// FinalizePaidOrder 在单一事务内完成订单 Paid -> Delivered 跃迁、账单写入与可选的业务扩展。
//
// 事务内的三步：
//  1. UPDATE orders SET status=Delivered, delivered_at=NOW, updated_at=NOW WHERE order_no=? AND status=Paid
//  2. INSERT INTO bills ...
//  3. 若 Config.FinalizeExtra 非 nil，在同一 tx 内调用，用于业务侧的权益发放（VIP 激活、积分入账等）。
//
// FinalizeExtra 返回 error 会整体回滚——调用方可以借此保证"订单状态 + 账单 + 权益"的强一致。
func (s *Store[O, M]) FinalizePaidOrder(ctx context.Context, order O, bill orderflow.BillSpec) error {
	now := time.Now()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Table(s.orderTable).
			Where(s.cols.OrderNo+" = ? AND "+s.cols.Status+" = ?", order.OrderNo(), orderflow.StatusPaid).
			Updates(map[string]any{
				s.cols.Status:      orderflow.StatusDelivered,
				s.cols.DeliveredAt: now,
				s.cols.UpdatedAt:   now,
			})
		if result.Error != nil {
			return fmt.Errorf("gormstore: finalize update order: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("gormstore: finalize: order %s not in paid state", order.OrderNo())
		}

		billModel := buildBill(bill)
		if err := tx.Table(s.billTable).Create(billModel).Error; err != nil {
			return fmt.Errorf("gormstore: finalize insert bill: %w", err)
		}

		if s.finalizeExtra != nil {
			if err := s.finalizeExtra(tx, order, billModel); err != nil {
				return fmt.Errorf("gormstore: finalize extra: %w", err)
			}
		}
		return nil
	})
}
