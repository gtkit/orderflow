package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/gtkit/orderflow"
)

// CloseWorker 消费延时队列，对到期 Pending 订单触发 Engine.Close。
//
// 并发模型：
//   - 主循环按 PollInterval 节拍，每次先 RequeueExpired 回收过期租约，再 ReserveExpired 拿一批任务；
//   - 每个任务派发到 MaxWorkers 上限的 goroutine 池处理（缓冲 channel 做信号量限流）；
//   - 处理成功后 Ack；失败记 error 日志，等下一轮 RequeueExpired 回收后重试；
//   - poll 连续失败时启用指数退避，避免日志风暴与连接池耗尽（Redis / DB 故障时最多每 30s 探活）；
//   - ctx 取消后 Run 用 sync.WaitGroup 精确等待所有在途 goroutine 结束。
type CloseWorker[O orderflow.OrderSnapshot] struct {
	engine *orderflow.Engine[O]
	queue  orderflow.DelayQueue
	logger *slog.Logger
	opts   CloseOptions
	pool   chan struct{}
	wg     sync.WaitGroup
	stats  statsRecorder
}

// Stats 返回 worker 运行时快照。线程安全，可从任意 goroutine 调用。
// 用于接入 Prometheus gauge（Inflight / LastBatchSize）、健康检查（LastPollAt 判断是否活着）。
func (w *CloseWorker[O]) Stats() Stats {
	return w.stats.snapshot()
}

// NewCloseWorker 构造 CloseWorker。opts 零值等同于所有字段取默认。
func NewCloseWorker[O orderflow.OrderSnapshot](engine *orderflow.Engine[O], opts CloseOptions) *CloseWorker[O] {
	opts = opts.withDefaults()
	return &CloseWorker[O]{
		engine: engine,
		queue:  engine.DelayQueue(),
		logger: engine.Logger(),
		opts:   opts,
		pool:   make(chan struct{}, opts.MaxWorkers),
	}
}

// Run 启动轮询循环，阻塞直到 ctx 取消。ctx 取消后等待所有已派发的任务收尾。
func (w *CloseWorker[O]) Run(ctx context.Context) {
	w.logger.InfoContext(ctx, "orderflow: close worker started")
	defer w.logger.InfoContext(ctx, "orderflow: close worker stopped")

	ticker := time.NewTicker(w.opts.PollInterval)
	defer ticker.Stop()

	var backoff time.Duration
	for {
		select {
		case <-ctx.Done():
			w.wg.Wait()
			return
		case <-ticker.C:
			if backoff > 0 {
				select {
				case <-ctx.Done():
					w.wg.Wait()
					return
				case <-time.After(backoff):
				}
			}
			if err := w.poll(ctx); err != nil {
				// 指数退避：上限 30s，避免 Redis 故障时持续每秒打日志 + 连接池耗尽
				if backoff == 0 {
					backoff = time.Second
				} else {
					backoff *= 2
				}
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			} else {
				backoff = 0
			}
		}
	}
}

// poll 执行一轮轮询：回收过期租约 → 预留到期任务 → 派发处理。
// 返回非 nil 表示轮询过程中有底层错误（Redis / 脚本），用于触发上层退避。
func (w *CloseWorker[O]) poll(ctx context.Context) error {
	start := w.stats.recordPollStart()
	batch := 0
	var pollErr error
	defer func() { w.stats.recordPollEnd(start, batch, pollErr) }()

	var lastErr error

	reclaimed, err := w.queue.RequeueExpired(ctx, w.opts.PollBatchSize)
	if err != nil {
		w.logger.ErrorContext(ctx, "orderflow: requeue expired failed",
			slog.Any("error", err),
		)
		lastErr = err
	} else if len(reclaimed) > 0 {
		w.logger.InfoContext(ctx, "orderflow: requeued expired inflight tasks",
			slog.Int("count", len(reclaimed)),
		)
	}

	orderNos, err := w.queue.ReserveExpired(ctx, w.opts.PollBatchSize, w.opts.PollLease)
	if err != nil {
		w.logger.ErrorContext(ctx, "orderflow: reserve expired failed",
			slog.Any("error", err),
		)
		pollErr = err
		return err
	}
	batch = len(orderNos)

	for _, orderNo := range orderNos {
		select {
		case <-ctx.Done():
			pollErr = lastErr
			return lastErr
		case w.pool <- struct{}{}:
		}
		w.wg.Add(1)
		w.stats.inflight.Add(1)
		go w.processOne(ctx, orderNo)
	}
	pollErr = lastErr
	return lastErr
}

// processOne 在独立 goroutine 内处理单个订单：Close -> Ack。
// defer 严格顺序：先释放 pool 槽 → 再 wg.Done，保证 Run 的 wg.Wait 返回时 pool 一定已清空。
// recover 是生产侧防御：用户 OnClosed 钩子或驱动层 panic 不应整进程崩溃。
func (w *CloseWorker[O]) processOne(ctx context.Context, orderNo string) {
	defer w.wg.Done()
	defer func() { <-w.pool }()
	defer w.stats.inflight.Add(-1)
	defer func() {
		if r := recover(); r != nil {
			w.logger.ErrorContext(ctx, "orderflow: panic in close worker recovered",
				slog.String("order_no", orderNo),
				slog.Any("panic", r),
			)
		}
	}()

	// closeCtx 使用 WithoutCancel：父 ctx（Run 收到 SIGTERM 后取消）不应直接打断
	// 在途的 gateway CloseOrder / CAS。让在途任务跑满自己的 CloseTimeout 预算，
	// Run 的 wg.Wait 会等到它们完成。避免 graceful shutdown 期间每个任务都 ctx.Canceled
	// 导致下次 Requeue 重复执行。
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.opts.CloseTimeout)
	defer cancel()

	if err := w.engine.Close(closeCtx, orderNo); err != nil {
		w.logger.ErrorContext(ctx, "orderflow: close order failed",
			slog.String("order_no", orderNo),
			slog.Any("error", err),
		)
		return
	}

	// Ack 必须即便 ctx 已取消也要完成（否则下次 RequeueExpired 会重跑一次已成功的 Close）。
	// context.WithoutCancel 保留 trace / request_id 等 value，但解耦取消信号；再叠加独立超时。
	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), w.opts.AckTimeout)
	defer ackCancel()

	acked, err := w.queue.Ack(ackCtx, orderNo)
	if err != nil {
		// Ack 失败是可容忍错误：下一轮 RequeueExpired 会把任务回收重跑，
		// engine.Close 本身对"已关闭订单"幂等（非 Pending 直接 skip）。
		w.logger.ErrorContext(ctx, "orderflow: ack close task failed, will be requeued",
			slog.String("order_no", orderNo),
			slog.Any("error", err),
		)
		return
	}
	if !acked {
		w.logger.WarnContext(ctx, "orderflow: ack missed inflight record",
			slog.String("order_no", orderNo),
		)
	}
}
