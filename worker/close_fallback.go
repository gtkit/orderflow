package worker

import (
	"context"
	"time"

	"github.com/gtkit/orderflow"
)

// CloseFallback 周期扫描 DB 中已过期但仍为 Pending 的订单，调用 Engine.Close 兜底。
// 典型场景：CloseWorker 所依赖的 Redis 延时队列发生数据丢失 / 写入丢失时的第二道防线。
type CloseFallback[O orderflow.OrderSnapshot] struct {
	engine *orderflow.Engine[O]
	logger orderflow.Logger
	opts   CloseFallbackOptions
}

// NewCloseFallback 构造 CloseFallback。
func NewCloseFallback[O orderflow.OrderSnapshot](engine *orderflow.Engine[O], opts CloseFallbackOptions) *CloseFallback[O] {
	return &CloseFallback[O]{
		engine: engine,
		logger: engine.Logger(),
		opts:   opts.withDefaults(),
	}
}

// Run 启动扫描循环，阻塞直到 ctx 取消。
func (f *CloseFallback[O]) Run(ctx context.Context) {
	f.logger.Info(ctx, "orderflow: close fallback scanner started")
	defer f.logger.Info(ctx, "orderflow: close fallback scanner stopped")

	ticker := time.NewTicker(f.opts.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.scan(ctx)
		}
	}
}

func (f *CloseFallback[O]) scan(ctx context.Context) {
	// 单次 scan 的 panic 不应挂死 scanner 主循环；记日志后等下一个 tick 重试。
	defer func() {
		if r := recover(); r != nil {
			f.logger.Error(ctx, "orderflow: panic in close fallback scan recovered",
				orderflow.Any("panic", r),
			)
		}
	}()

	orderNos, err := f.engine.FindExpiredPending(ctx, f.opts.BatchSize)
	if err != nil {
		f.logger.Error(ctx, "orderflow: close fallback scan failed",
			orderflow.Any("error", err),
		)
		return
	}
	if len(orderNos) == 0 {
		return
	}

	f.logger.Info(ctx, "orderflow: close fallback found expired orders",
		orderflow.Int("count", len(orderNos)),
	)

	for _, orderNo := range orderNos {
		taskCtx, cancel := context.WithTimeout(ctx, f.opts.PerTaskTimeout)
		if err := f.engine.Close(taskCtx, orderNo); err != nil {
			f.logger.Error(ctx, "orderflow: close fallback close failed",
				orderflow.String("order_no", orderNo),
				orderflow.Any("error", err),
			)
		}
		cancel()
	}
}
