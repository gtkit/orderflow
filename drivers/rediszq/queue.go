package rediszq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultMaxBatchSize = 500

// Lua 脚本：使用 Redis 服务端时间原子取出到期成员并删除。
//
// 多实例安全：ZRANGEBYSCORE + ZREM 在同一个 Lua 脚本中执行，
// Redis 单线程保证原子性，同一个 member 只会被一个调用者取到。
var fetchExpiredScript = redis.NewScript(`
	local key = KEYS[1]
	local limit = tonumber(ARGV[1])
	local now = tonumber(ARGV[2])

	local members = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'LIMIT', 0, limit)
	if #members == 0 then
		return {}
	end

	local fetched = {}
	for _, member in ipairs(members) do
		if redis.call('ZREM', key, member) == 1 then
			fetched[#fetched + 1] = member
		end
	end
	return fetched
`)

// Lua 脚本：原子预留到期成员到 processing ZSET。
//
// 被预留的 member 会从待执行队列移到 processing 队列，
// score 改为 lease 到期时间；消费者成功处理后需显式 Ack，
// 否则可通过 RequeueExpired 将超时未 Ack 的任务重新放回待执行队列。
var reserveExpiredScript = redis.NewScript(`
	local pending = KEYS[1]
	local processing = KEYS[2]
	local lease_ms = tonumber(ARGV[1])
	local limit = tonumber(ARGV[2])
	local now = tonumber(ARGV[3])

	local lease_until = now + lease_ms

	local members = redis.call('ZRANGEBYSCORE', pending, '-inf', now, 'LIMIT', 0, limit)
	if #members == 0 then
		return {}
	end

	local reserved = {}
	for _, member in ipairs(members) do
		if redis.call('ZREM', pending, member) == 1 then
			redis.call('ZADD', processing, lease_until, member)
			reserved[#reserved + 1] = member
		end
	end

	return reserved
`)

// Lua 脚本：将 processing 中 lease 过期的任务重新放回待执行队列。
var requeueExpiredScript = redis.NewScript(`
	local pending = KEYS[1]
	local processing = KEYS[2]
	local limit = tonumber(ARGV[1])
	local now = tonumber(ARGV[2])

	local members = redis.call('ZRANGEBYSCORE', processing, '-inf', now, 'LIMIT', 0, limit)
	if #members == 0 then
		return {}
	end

	local requeued = {}
	for _, member in ipairs(members) do
		if redis.call('ZREM', processing, member) == 1 then
			redis.call('ZADD', pending, now, member)
			requeued[#requeued + 1] = member
		end
	end

	return requeued
`)

// Lua 脚本：同时从 pending / processing 中移除任务。
var removeScript = redis.NewScript(`
	local pending = KEYS[1]
	local processing = KEYS[2]
	local member = ARGV[1]

	local removed = 0
	removed = removed + redis.call('ZREM', pending, member)
	removed = removed + redis.call('ZREM', processing, member)
	return removed
`)

// Queue 是基于 Redis ZSET 的延迟队列。
type Queue struct {
	rdb            RedisClient
	key            string
	processingKey  string
	maxBatch       int
	defaultTimeout time.Duration
}

// RedisClient 是 Queue 需要的 Redis 能力集。
//
// *redis.Client、*redis.ClusterClient 和 *redis.Ring 均满足此接口。
type RedisClient interface {
	redis.Cmdable
	redis.Scripter
}

// Option 用于配置 Queue 的可选行为。
type Option func(*Queue) error

// WithMaxBatch 设置单次脚本操作允许处理的最大任务数。
func WithMaxBatch(size int) Option {
	return func(q *Queue) error {
		if size <= 0 {
			return errors.New("rediszq: max batch must be positive")
		}
		q.maxBatch = size
		return nil
	}
}

// WithDefaultTimeout 为未显式携带 deadline 的调用补一个默认超时。
func WithDefaultTimeout(timeout time.Duration) Option {
	return func(q *Queue) error {
		if timeout <= 0 {
			return errors.New("rediszq: timeout must be positive")
		}
		q.defaultTimeout = timeout
		return nil
	}
}

// New 创建延迟队列。key 是 Redis ZSET 的 key，不同业务用不同 key 隔离。
func New(rdb RedisClient, key string, opts ...Option) (*Queue, error) {
	if rdb == nil {
		return nil, errors.New("rediszq: nil redis client")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, errors.New("rediszq: empty key")
	}

	queue := &Queue{
		rdb:           rdb,
		key:           key,
		processingKey: key + ":processing",
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(queue); err != nil {
			return nil, err
		}
	}

	return queue, nil
}

// MustNew 创建延迟队列，初始化失败时 panic。
//
// **仅限 init() / main() 启动阶段使用**——配置错误时让进程启动失败比返回 error
// 更明确（与 stdlib 的 `regexp.MustCompile` / `template.Must` 同模式）。
//
// **运行时禁用**（请求处理路径、goroutine 内、定时任务里）：运行时调用 panic
// 会冲破业务主流程，违反库代码"零 panic"约束。需要在运行时构造时使用 New 并显式
// 处理 error。
func MustNew(rdb RedisClient, key string, opts ...Option) *Queue {
	queue, err := New(rdb, key, opts...)
	if err != nil {
		panic(fmt.Errorf("rediszq: MustNew: %w", err))
	}
	return queue
}

// Enqueue 投递延迟任务。member 在 executeAt 时间点到期后可被 ReserveExpired / FetchExpired 取出。
// 使用 NX 语义：member 已存在时不覆盖（天然去重）。返回值表示是否实际入队。
func (q *Queue) Enqueue(ctx context.Context, member string, executeAt time.Time) (bool, error) {
	member, err := normalizeMember(member, "enqueue")
	if err != nil {
		return false, err
	}
	if err := q.validate(); err != nil {
		return false, err
	}
	ctx, cancel := q.operationContext(ctx)
	defer cancel()

	added, err := q.rdb.ZAddNX(ctx, q.key, redis.Z{
		Score:  scoreForTime(executeAt),
		Member: member,
	}).Result()
	if err != nil {
		return false, fmt.Errorf("rediszq enqueue: %w", err)
	}
	return added > 0, nil
}

// Remove 移除任务。Remove 会同时清理 pending 和 processing，避免消费者已预留但尚未 Ack
// 的任务继续被重新投递。
func (q *Queue) Remove(ctx context.Context, member string) error {
	member, err := normalizeMember(member, "remove")
	if err != nil {
		return err
	}
	if err := q.validate(); err != nil {
		return err
	}
	ctx, cancel := q.operationContext(ctx)
	defer cancel()

	if _, err := removeScript.Run(ctx, q.rdb, []string{q.key, q.processingKey}, member).Result(); err != nil {
		return fmt.Errorf("rediszq remove: %w", err)
	}
	return nil
}

// FetchExpired 原子取出所有到期的 member。
//
// 这是"取出即删除"语义，适用于不需要 Ack 的轻量场景。
// 生产消费端应优先使用 ReserveExpired + Ack / RequeueExpired。
func (q *Queue) FetchExpired(ctx context.Context, batchSize int) ([]string, error) {
	if batchSize <= 0 {
		return nil, errors.New("rediszq fetch: batch size must be positive")
	}
	if err := q.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := q.operationContext(ctx)
	defer cancel()
	batchSize = q.clampBatch(batchSize)

	result, err := fetchExpiredScript.Run(ctx, q.rdb, []string{q.key}, batchSize, time.Now().UnixMilli()).StringSlice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("rediszq fetch: %w", err)
	}
	return result, nil
}

// ReserveExpired 原子预留到期任务，并把它们移动到 processing 队列。
//
// 预留成功的任务必须在处理完成后调用 Ack；若消费者崩溃或未及时 Ack，
// 可通过 RequeueExpired 将 lease 过期任务重新放回待执行队列。
func (q *Queue) ReserveExpired(ctx context.Context, batchSize int, lease time.Duration) ([]string, error) {
	if batchSize <= 0 {
		return nil, errors.New("rediszq reserve: batch size must be positive")
	}
	if lease <= 0 {
		return nil, errors.New("rediszq reserve: lease must be positive")
	}
	if err := q.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := q.operationContext(ctx)
	defer cancel()
	batchSize = q.clampBatch(batchSize)

	result, err := reserveExpiredScript.Run(
		ctx,
		q.rdb,
		[]string{q.key, q.processingKey},
		lease.Milliseconds(),
		batchSize,
		time.Now().UnixMilli(),
	).StringSlice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("rediszq reserve: %w", err)
	}
	return result, nil
}

// Ack 确认任务已处理完成。返回值表示是否实际移除了 processing 中的任务。
func (q *Queue) Ack(ctx context.Context, member string) (bool, error) {
	member, err := normalizeMember(member, "ack")
	if err != nil {
		return false, err
	}
	if err := q.validate(); err != nil {
		return false, err
	}
	ctx, cancel := q.operationContext(ctx)
	defer cancel()

	removed, err := q.rdb.ZRem(ctx, q.processingKey, member).Result()
	if err != nil {
		return false, fmt.Errorf("rediszq ack: %w", err)
	}
	return removed > 0, nil
}

// RequeueExpired 将 processing 中 lease 已过期的任务重新放回待执行队列。
func (q *Queue) RequeueExpired(ctx context.Context, batchSize int) ([]string, error) {
	if batchSize <= 0 {
		return nil, errors.New("rediszq requeue: batch size must be positive")
	}
	if err := q.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := q.operationContext(ctx)
	defer cancel()
	batchSize = q.clampBatch(batchSize)

	result, err := requeueExpiredScript.Run(
		ctx,
		q.rdb,
		[]string{q.key, q.processingKey},
		batchSize,
		time.Now().UnixMilli(),
	).StringSlice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("rediszq requeue: %w", err)
	}
	return result, nil
}

// Len 返回待执行队列中待处理的任务数（监控用）。
func (q *Queue) Len(ctx context.Context) (int64, error) {
	if err := q.validate(); err != nil {
		return 0, err
	}
	ctx, cancel := q.operationContext(ctx)
	defer cancel()

	count, err := q.rdb.ZCard(ctx, q.key).Result()
	if err != nil {
		return 0, fmt.Errorf("rediszq len: %w", err)
	}
	return count, nil
}

// ProcessingLen 返回 processing 队列中处于预留中的任务数。
func (q *Queue) ProcessingLen(ctx context.Context) (int64, error) {
	if err := q.validate(); err != nil {
		return 0, err
	}
	ctx, cancel := q.operationContext(ctx)
	defer cancel()

	count, err := q.rdb.ZCard(ctx, q.processingKey).Result()
	if err != nil {
		return 0, fmt.Errorf("rediszq processing len: %w", err)
	}
	return count, nil
}

// ExpiredProcessingCount 返回 processing 中 lease 已过期但尚未回队列的任务数。
func (q *Queue) ExpiredProcessingCount(ctx context.Context) (int64, error) {
	if err := q.validate(); err != nil {
		return 0, err
	}
	ctx, cancel := q.operationContext(ctx)
	defer cancel()

	count, err := q.rdb.ZCount(ctx, q.processingKey, "-inf", fmt.Sprintf("%d", time.Now().UnixMilli())).Result()
	if err != nil {
		return 0, fmt.Errorf("rediszq expired processing count: %w", err)
	}
	return count, nil
}

// Stats 返回队列统计信息，便于运维观测。
func (q *Queue) Stats(ctx context.Context) (*Stats, error) {
	if err := q.validate(); err != nil {
		return nil, err
	}

	pending, err := q.Len(ctx)
	if err != nil {
		return nil, err
	}
	processing, err := q.ProcessingLen(ctx)
	if err != nil {
		return nil, err
	}
	expired, err := q.ExpiredProcessingCount(ctx)
	if err != nil {
		return nil, err
	}

	return &Stats{
		QueueName:         q.key,
		Pending:           pending,
		Processing:        processing,
		ExpiredProcessing: expired,
	}, nil
}

// Stats 是队列当前状态的快照。
type Stats struct {
	QueueName         string
	Pending           int64
	Processing        int64
	ExpiredProcessing int64
}

func (q *Queue) validate() error {
	if q == nil {
		return errors.New("rediszq: nil queue")
	}
	return nil
}

func normalizeMember(member, action string) (string, error) {
	member = strings.TrimSpace(member)
	if member == "" {
		return "", fmt.Errorf("rediszq %s: empty member", action)
	}
	return member, nil
}

func (q *Queue) clampBatch(size int) int {
	limit := defaultMaxBatchSize
	if q != nil && q.maxBatch > 0 {
		limit = q.maxBatch
	}
	return min(size, limit)
}

func (q *Queue) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if q == nil || q.defaultTimeout <= 0 {
		return ctx, func() {}
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, q.defaultTimeout)
}

func scoreForTime(value time.Time) float64 {
	return float64(value.UnixMilli())
}
