package orderflow

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Close 关闭一个 Pending 订单（幂等）。
//
// 非 Pending 状态、尚未过期、订单不存在这三种情况都会 skip 并返回 nil——
// 让外层调度器（worker）不必判断，直接无脑调用。
//
// 成功路径：调用支付网关 CloseOrder（带 3 次重试 + 网关特定的"可忽略错误"判断），
// 然后 CAS 把本地订单推进到 Closed。
func (e *Engine[O]) Close(ctx context.Context, orderNo string) (err error) {
	start := time.Now()
	defer func() {
		e.observer.Duration(ctx, OpClose, time.Since(start), err)
	}()

	order, found, err := e.store.GetByNo(ctx, orderNo)
	if err != nil {
		return fmt.Errorf("orderflow: query order for close: %w", err)
	}
	if !found {
		e.logger.WarnContext(ctx, "orderflow: close skipped: order not found",
			slog.String("order_no", orderNo),
		)
		return nil
	}

	if order.Status() != StatusPending {
		e.logger.DebugContext(ctx, "orderflow: close skipped: order not pending",
			slog.String("order_no", orderNo),
			slog.String("status", order.Status().String()),
		)
		return nil
	}

	if time.Now().Before(order.ExpireAt()) {
		e.logger.DebugContext(ctx, "orderflow: close skipped: order not expired yet",
			slog.String("order_no", orderNo),
		)
		return nil
	}

	if err := e.afterClose(ctx, order); err != nil {
		e.appendLog(ctx, order, StatusPending, StatusPending, "system",
			"gateway close failed: "+err.Error())
		return fmt.Errorf("orderflow: gateway close: %w", err)
	}

	affected, err := e.store.CASClose(ctx, orderNo)
	if err != nil {
		return fmt.Errorf("orderflow: cas close: %w", err)
	}
	if affected == 0 {
		current, ok, qErr := e.store.GetByNo(ctx, orderNo)
		if qErr != nil {
			return fmt.Errorf("orderflow: recheck after close race: %w", qErr)
		}
		if ok && current.Status() == StatusPaid {
			e.appendLog(ctx, current, StatusPending, StatusPaid, "system",
				"payment won race during timeout close")
		}
		return nil
	}

	e.publishStatus(ctx, order.OrderToken(), order.UserID(), StatusClosed, order.ExpireAt())
	e.appendLog(ctx, order, StatusPending, StatusClosed, "system", "closed: payment timeout")

	e.observer.Event(ctx, EventOrderClosed, order.OrderNo(), map[string]any{
		"reason": string(ClosedReasonTimeout),
	})
	if e.onClosed != nil {
		e.safeHook(ctx, "OnClosed", order.OrderNo(), func() {
			e.onClosed(ctx, order, ClosedReasonTimeout)
		})
	}
	e.logger.InfoContext(ctx, "orderflow: order closed",
		slog.String("order_no", orderNo),
	)
	return nil
}

// CloseByUser 让用户主动关闭自己的订单。
// 相比 Close：先校验 order.UserID() == userID（不匹配返回 ErrOrderForbidden），
// 再走标准 Close 流程。适用于"我的订单 → 取消"这类用户接口。
//
// 注意：仅校验 UserID 归属；订单状态、过期时间等由下层 Close 检查。
// 未过期的 Pending 订单仍会被 Close skip（Close 对非过期订单幂等跳过），
// 若业务需要"立即取消未过期订单"的语义，应使用 CloseByAdmin 或调用 Store.CASClose。
func (e *Engine[O]) CloseByUser(ctx context.Context, userID int64, orderNo string) error {
	order, found, err := e.store.GetByNo(ctx, orderNo)
	if err != nil {
		return fmt.Errorf("orderflow: query order for CloseByUser: %w", err)
	}
	if !found {
		return ErrOrderNotFound
	}
	if order.UserID() != userID {
		return ErrOrderForbidden
	}
	return e.Close(ctx, orderNo)
}

// CloseByAdmin 强制关闭订单，**绕过 ExpireAt 守卫**。
//
// 适用场景：管理员后台"强制取消"、风控系统"异常订单关闭"、客服处理用户申诉。
// 与 Close 的差异：不检查 order.ExpireAt() 是否到期——未过期的 Pending 订单也能直接关。
//
// 仍然保留的安全约束：
//   - 仅 Pending 订单可被关闭（Paid/Delivered 状态调用本方法返回 nil 跳过，避免误关已支付订单）；
//   - 网关 CloseOrder 仍会被调用 + 重试；
//   - 状态推送、流水、OnClosed 钩子全部触发，actor 标记为 admin。
//
// reason 参数会写入流水的 remark 字段，便于事后审计。建议传业务定义的"风控规则 ID"
// 或"客服工单号"，便于追溯。
func (e *Engine[O]) CloseByAdmin(ctx context.Context, orderNo, reason string) error {
	order, found, err := e.store.GetByNo(ctx, orderNo)
	if err != nil {
		return fmt.Errorf("orderflow: query order for CloseByAdmin: %w", err)
	}
	if !found {
		return ErrOrderNotFound
	}
	if order.Status() != StatusPending {
		e.logger.InfoContext(ctx, "orderflow: admin close skipped: order not pending",
			"order_no", orderNo,
			"status", order.Status().String(),
		)
		return nil
	}

	if afterErr := e.afterClose(ctx, order); afterErr != nil {
		e.appendLog(ctx, order, StatusPending, StatusPending, "admin",
			"gateway close failed during admin close: "+afterErr.Error())
		return fmt.Errorf("orderflow: admin gateway close: %w", afterErr)
	}

	affected, err := e.store.CASClose(ctx, orderNo)
	if err != nil {
		return fmt.Errorf("orderflow: admin cas close: %w", err)
	}
	if affected == 0 {
		// 状态被并发推进——常见情形：支付回调先到。Close 路径的标准行为是 skip。
		return nil
	}

	e.publishStatus(ctx, order.OrderToken(), order.UserID(), StatusClosed, order.ExpireAt())
	remark := "admin force close"
	if reason != "" {
		remark = "admin force close: " + reason
	}
	e.appendLog(ctx, order, StatusPending, StatusClosed, "admin", remark)
	e.observer.Event(ctx, EventOrderClosed, order.OrderNo(), map[string]any{
		"reason": string(ClosedReasonManual),
		"actor":  "admin",
	})
	if e.onClosed != nil {
		e.safeHook(ctx, "OnClosed", order.OrderNo(), func() {
			e.onClosed(ctx, order, ClosedReasonManual)
		})
	}
	return nil
}

// afterClose 调用支付网关关闭订单，带 3 次重试和可忽略错误容忍。
func (e *Engine[O]) afterClose(ctx context.Context, order O) error {
	channel := e.resolveChannelOf(order.PayMethod())
	_, err := retryN(ctx, 3, 100*time.Millisecond, func() (struct{}, error) {
		return struct{}{}, e.gateway.CloseOrder(ctx, channel, order.OrderNo())
	})
	if err == nil {
		return nil
	}
	if e.gateway.IsIgnorableCloseError(channel, err) {
		e.logger.DebugContext(ctx, "orderflow: gateway close returned ignorable error, continue local close",
			slog.String("order_no", order.OrderNo()),
			slog.String("channel", string(channel)),
			slog.Any("error", err),
		)
		return nil
	}
	return err
}

// MaxFindLimit 是 FindExpiredPending / FindPaidUndelivered 的硬上限。
// 调用方传入超过此值会被 clamp 到该值——避免无节制的 limit 把百万行订单一次拉回来撑爆内存。
// 业务后台分页接口应自行决定 limit，此处仅做防御性兜底。
const MaxFindLimit = 1000

// FindExpiredPending 返回已过期但仍为 Pending 的订单号，供 fallback worker 扫描使用。
// limit <= 0 或 > MaxFindLimit 时 clamp 到 MaxFindLimit。
func (e *Engine[O]) FindExpiredPending(ctx context.Context, limit int) ([]string, error) {
	return e.store.FindExpiredPending(ctx, clampLimit(limit))
}

// FindPaidUndelivered 返回已支付但未进入 Delivered 的订单号，供 fallback worker 扫描使用。
// limit <= 0 或 > MaxFindLimit 时 clamp 到 MaxFindLimit。
func (e *Engine[O]) FindPaidUndelivered(ctx context.Context, limit int) ([]string, error) {
	return e.store.FindPaidUndelivered(ctx, clampLimit(limit))
}

func clampLimit(limit int) int {
	if limit <= 0 || limit > MaxFindLimit {
		return MaxFindLimit
	}
	return limit
}

// ReconcilePaid 对 Paid 但未 Delivered 的订单执行补偿，用于 OnPaid 钩子失败后兜底。
//
// 订单必须已经持有 TradeNo 和 PaidAt（支付回调已经写入），否则返回错误让调用方发现。
// 状态为 Delivered / Completed 时直接返回 nil（已完成）；非 Paid 状态下 skip 并返回 nil。
func (e *Engine[O]) ReconcilePaid(ctx context.Context, orderNo string) (err error) {
	start := time.Now()
	defer func() {
		e.observer.Duration(ctx, OpReconcilePaid, time.Since(start), err)
	}()

	order, found, err := e.store.GetByNo(ctx, orderNo)
	if err != nil {
		return fmt.Errorf("orderflow: query order for reconcile: %w", err)
	}
	if !found {
		e.logger.WarnContext(ctx, "orderflow: reconcile skipped: order not found",
			slog.String("order_no", orderNo),
		)
		return nil
	}

	switch order.Status() {
	case StatusDelivered, StatusCompleted:
		return nil
	case StatusPaid:
		// 继续执行补偿
	default:
		e.logger.InfoContext(ctx, "orderflow: reconcile skipped: order not in paid state",
			slog.String("order_no", order.OrderNo()),
			slog.String("status", order.Status().String()),
		)
		return nil
	}

	paidAt, hasPaidAt := order.PaidAt()
	if !hasPaidAt || order.TradeNo() == "" {
		return fmt.Errorf("orderflow: reconcile missing payment metadata for order %s", order.OrderNo())
	}

	notify := NotifyResult{
		OutTradeNo:    order.OrderNo(),
		TransactionID: order.TradeNo(),
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   order.PayAmount(),
		PaidAt:        paidAt,
		Channel:       e.resolveChannelOf(order.PayMethod()),
	}
	return e.finalizeDelivery(ctx, order, notify)
}
