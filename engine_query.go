package orderflow

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// PollStatus 查询订单当前状态。
//
// 读路径：缓存优先，命中则不回源 DB；缓存 miss 或缓存故障时回源 DB 并尝试回填缓存。
// token 存在但 UserID 不匹配时返回 ErrOrderForbidden；订单不存在返回 ErrOrderNotFound。
//
// 缓存故障处理：Cache.Get 返回 err（区别于 miss）会被升级为 ALERT 日志 + Observer
// 异常事件——让运维感知 cache 抖动而不是静默回源 DB 引发主库压力风暴。回源逻辑
// 仍然继续，保证用户读路径的可用性。
func (e *Engine[O]) PollStatus(ctx context.Context, orderToken string, userID int64) (result *StatusResult, err error) {
	start := time.Now()
	defer func() {
		e.observer.Duration(ctx, OpPollStatus, time.Since(start), err)
	}()

	cached, hit, cacheErr := e.cache.Get(ctx, orderToken)
	switch {
	case cacheErr != nil:
		// 区别于 miss：err 通常意味着 Redis 网络抖动或解析异常，需要可观测。
		e.logger.ErrorContext(ctx, "orderflow: ALERT poll cache get failed, falling back to db",
			slog.String("order_token", orderToken),
			slog.Any("error", cacheErr),
		)
		e.observer.Event(ctx, EventAnomaly, "", map[string]any{
			"kind":        "poll_cache_get_failed",
			"order_token": orderToken,
			"reason":      cacheErr.Error(),
		})
	case hit:
		if cached.UserID != userID {
			err = ErrOrderForbidden
			return nil, err
		}
		return &StatusResult{Status: cached.Status, StatusText: cached.Status.String()}, nil
	}

	order, found, qErr := e.store.GetByToken(ctx, orderToken)
	if qErr != nil {
		err = fmt.Errorf("orderflow: query order by token: %w", qErr)
		return nil, err
	}
	if !found {
		err = ErrOrderNotFound
		return nil, err
	}
	if order.UserID() != userID {
		err = ErrOrderForbidden
		return nil, err
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
