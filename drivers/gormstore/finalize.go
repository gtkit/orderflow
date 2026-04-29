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
//
// affected=0 时返回的错误会附带订单当前真实状态，便于运维区分"已 Delivered（重入）"
// 与"被并发关闭（异常）"两种情形。
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
			// 二次 SELECT 把当前状态读出来——错误信息里只说"not in paid state"
			// 会让运维难以判断到底是已 Delivered（重入）还是被并发 Closed（异常）。
			var currentStatus orderflow.OrderStatus
			selErr := tx.Table(s.orderTable).
				Select(s.cols.Status).
				Where(s.cols.OrderNo+" = ?", order.OrderNo()).
				Limit(1).
				Scan(&currentStatus).Error
			if selErr != nil {
				return fmt.Errorf("gormstore: finalize: order %s not in paid state (recheck failed: %w)",
					order.OrderNo(), selErr)
			}
			return fmt.Errorf("gormstore: finalize: order %s not in paid state (current=%s)",
				order.OrderNo(), currentStatus.String())
		}

		// 可选：在同一事务内回查 channel_id 列补到 bill spec，让"按渠道对账"开箱可用。
		//
		// 触发条件（**必须三者皆满足**）：
		//   1. ColumnMap.ChannelID 已显式配置（非空）——opt-in，确保业务表确实有该列；
		//   2. BillSpec.ChannelID 为零（业务方未通过自定义路径填值）；
		//   3. 订单存在该列且可读取。
		//
		// 不满足时跳过 SELECT，spec.ChannelID 保留 BillSpec 传入值（通常为 0）。
		// 回查结果合并到 spec 副本（值类型拷贝），同时传给 BillWriter 与 FinalizeExtra，
		// 保证两处看到的 channel_id 完全一致。
		spec := bill
		if s.cols.ChannelID != "" && spec.ChannelID == 0 {
			var channelID int64
			if err := tx.Table(s.orderTable).
				Select(s.cols.ChannelID).
				Where(s.cols.OrderNo+" = ?", order.OrderNo()).
				Limit(1).
				Scan(&channelID).Error; err != nil {
				return fmt.Errorf("gormstore: finalize lookup channel_id: %w", err)
			}
			spec.ChannelID = channelID
		}
		if err := s.billWriter.Write(tx, spec); err != nil {
			return err
		}

		if s.finalizeExtra != nil {
			if err := s.finalizeExtra(tx, order, spec); err != nil {
				return fmt.Errorf("gormstore: finalize extra: %w", err)
			}
		}
		return nil
	})
}
