package orderflow

import (
	"context"
	"fmt"
	"log/slog"
)

// PollStatus 查询订单当前状态。
//
// 读路径：缓存优先，命中则不回源 DB；缓存 miss 时回源 DB 并回填缓存。
// token 存在但 UserID 不匹配时返回 ErrOrderForbidden；订单不存在返回 ErrOrderNotFound。
func (e *Engine[O]) PollStatus(ctx context.Context, orderToken string, userID int64) (*StatusResult, error) {
	if cached, hit, err := e.cache.Get(ctx, orderToken); err == nil && hit {
		if cached.UserID != userID {
			return nil, ErrOrderForbidden
		}
		return &StatusResult{Status: cached.Status, StatusText: cached.Status.String()}, nil
	}

	order, found, err := e.store.GetByToken(ctx, orderToken)
	if err != nil {
		return nil, fmt.Errorf("orderflow: query order by token: %w", err)
	}
	if !found {
		return nil, ErrOrderNotFound
	}
	if order.UserID() != userID {
		return nil, ErrOrderForbidden
	}

	if setErr := e.cache.Set(ctx, orderToken, order.UserID(), order.Status(), order.ExpireAt()); setErr != nil {
		e.logger.WarnContext(ctx, "orderflow: backfill status cache failed after db lookup",
			slog.String("order_token", orderToken),
			slog.Any("error", setErr),
		)
	}

	return &StatusResult{Status: order.Status(), StatusText: order.Status().String()}, nil
}

// Timeline 返回订单的状态变更流水，用于客户端详情页展示。
func (e *Engine[O]) Timeline(ctx context.Context, orderToken string, userID int64) (*Timeline, error) {
	order, found, err := e.store.GetByToken(ctx, orderToken)
	if err != nil {
		return nil, fmt.Errorf("orderflow: query order by token: %w", err)
	}
	if !found {
		return nil, ErrOrderNotFound
	}
	if order.UserID() != userID {
		return nil, ErrOrderForbidden
	}

	entries, err := e.store.ListLogsByOrderNo(ctx, order.OrderNo())
	if err != nil {
		return nil, fmt.Errorf("orderflow: list order logs: %w", err)
	}

	return &Timeline{
		OrderToken: order.OrderToken(),
		OrderNo:    order.OrderNo(),
		Status:     order.Status(),
		Entries:    entries,
	}, nil
}

// ListUserOrders 返回用户的订单列表。
// 字段裁剪与排序由 Store driver 决定，核心包不做处理。
func (e *Engine[O]) ListUserOrders(ctx context.Context, userID int64) ([]O, error) {
	return e.store.ListByUser(ctx, userID)
}

// Subscribe 订阅订单状态变更推送，直接透传到 StatusStream driver。
func (e *Engine[O]) Subscribe(ctx context.Context, orderToken string) (Subscription, error) {
	return e.stream.Subscribe(ctx, orderToken)
}
