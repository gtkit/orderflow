package orderflow

import (
	"context"
	"time"
)

// DelayQueue 为订单提供"到点触发关闭"的延时调度能力。
//
// 语义约定：
//   - Enqueue 入队，executeAt 指定触发时刻；返回的 bool 表示是否是新记录（幂等入队）；
//   - ReserveExpired 以租约（lease）方式批量取出到期任务，期间若调用方未 Ack，
//     租约过期后可由 RequeueExpired 重新放回就绪队列，防止 worker 崩溃导致丢失；
//   - Ack 确认任务处理完毕；返回 bool 表示 inflight 记录是否存在；
//   - Remove 无条件移除记录，用于"订单已被支付或手工关闭"的清理。
type DelayQueue interface {
	Enqueue(ctx context.Context, member string, executeAt time.Time) (bool, error)
	Remove(ctx context.Context, member string) error
	ReserveExpired(ctx context.Context, batchSize int, lease time.Duration) ([]string, error)
	Ack(ctx context.Context, member string) (bool, error)
	RequeueExpired(ctx context.Context, batchSize int) ([]string, error)
}
