package worker

import (
	"context"
	"time"

	"github.com/gtkit/orderflow"
)

// DeliveryFallback 周期扫描 Paid 但未 Delivered 的订单，调用 Engine.ReconcilePaid 补偿。
// 典型场景：OnPaid 钩子临时失败（DB 短时不可用 / 网络抖动）后，scanner 兜底让订单最终进入 Delivered。
//
// **重试边界与告警**：本扫描器**没有内置的最大重试次数 / 指数退避**——只要订单仍处于
// Paid 未 Delivered 且未被人工介入，每个扫描周期都会再次调用 ReconcilePaid。设计上
// 假定业务方的 OnPaid 钩子是幂等的（见 orderflow.OnPaidHook 的强约束），且业务系统
// 故障在分钟级内会恢复。
//
// 长期失败（OnPaid 钩子持续返回错误，例如下游权益系统 outage / 业务 bug 未修）会导致
// 订单永远卡在 Paid，**库不主动转入终态**。运维侧告警的推荐做法：
//
//   - 在 Observer 上聚合 EventAnomaly + AnomalyDeliveryFailed 的计数（配合
//     Prometheus / Datadog 等）；
//   - 对同一 orderNo 在 N 分钟内出现 ≥ M 次的情形配置告警，转人工介入；
//   - 必要时在业务侧主动调 Engine.CloseByAdmin 收口，并在权益侧执行补偿。
//
// 库刻意不在此处持久化 retry 计数，避免引入新的 schema 列与额外的状态机分支。
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
