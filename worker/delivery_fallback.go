package worker

import (
	"context"
	"time"

	"github.com/gtkit/orderflow"
)

// DeliveryFallback 周期扫描 Paid 但未 Delivered 的订单，调用 Engine.ReconcilePaid 补偿。
// 典型场景：OnPaid 钩子临时失败（DB 短时不可用 / 网络抖动）后，scanner 兜底让订单最终进入 Delivered。
type DeliveryFallback[O orderflow.OrderSnapshot] struct {
	engine *orderflow.Engine[O]
	logger orderflow.Logger
	opts   DeliveryFallbackOptions
}

// NewDeliveryFallback 构造 DeliveryFallback。
func NewDeliveryFallback[O orderflow.OrderSnapshot](engine *orderflow.Engine[O], opts DeliveryFallbackOptions) *DeliveryFallback[O] {
	return &DeliveryFallback[O]{
		engine: engine,
		logger: engine.Logger(),
		opts:   opts.withDefaults(),
	}
}

// Run 启动扫描循环，阻塞直到 ctx 取消。
func (f *DeliveryFallback[O]) Run(ctx context.Context) {
	f.logger.Info(ctx, "orderflow: delivery fallback scanner started")
	defer f.logger.Info(ctx, "orderflow: delivery fallback scanner stopped")

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

func (f *DeliveryFallback[O]) scan(ctx context.Context) {
	// 单次 scan 的 panic 不应挂死 scanner 主循环；记日志后等下一个 tick 重试。
	defer func() {
		if r := recover(); r != nil {
			f.logger.Error(ctx, "orderflow: panic in delivery fallback scan recovered",
				orderflow.Any("panic", r),
			)
		}
	}()

	orderNos, err := f.engine.FindPaidUndelivered(ctx, f.opts.BatchSize)
	if err != nil {
		f.logger.Error(ctx, "orderflow: delivery fallback scan failed",
			orderflow.Any("error", err),
		)
		return
	}
	if len(orderNos) == 0 {
		return
	}

	f.logger.Info(ctx, "orderflow: delivery fallback found paid-undelivered",
		orderflow.Int("count", len(orderNos)),
	)

	for _, orderNo := range orderNos {
		taskCtx, cancel := context.WithTimeout(ctx, f.opts.PerTaskTimeout)
		if err := f.engine.ReconcilePaid(taskCtx, orderNo); err != nil {
			f.logger.Error(ctx, "orderflow: delivery fallback reconcile failed",
				orderflow.String("order_no", orderNo),
				orderflow.Any("error", err),
			)
		}
		cancel()
	}
}
