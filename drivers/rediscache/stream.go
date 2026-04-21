package rediscache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/gtkit/orderflow"
	"github.com/redis/go-redis/v9"
)

const defaultStreamKeyPrefix = "orderflow:status:events:"

// StatusStream 是基于 Redis Pub/Sub 的 orderflow.StatusStream 实现。
type StatusStream struct {
	rdb       *redis.Client
	keyPrefix string
}

// StreamOption 是 NewStatusStream 的可选配置。
type StreamOption func(*StatusStream)

// WithStreamKeyPrefix 覆盖频道 key 前缀（默认 "orderflow:status:events:"）。
func WithStreamKeyPrefix(prefix string) StreamOption {
	return func(s *StatusStream) { s.keyPrefix = prefix }
}

// NewStatusStream 构造 StatusStream。rdb 必填。
func NewStatusStream(rdb *redis.Client, opts ...StreamOption) *StatusStream {
	s := &StatusStream{
		rdb:       rdb,
		keyPrefix: defaultStreamKeyPrefix,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

var _ orderflow.StatusStream = (*StatusStream)(nil)

// Publish 将订单状态变更广播到 Redis Pub/Sub 频道。
func (s *StatusStream) Publish(ctx context.Context, orderToken string, status orderflow.OrderStatus) error {
	if strings.TrimSpace(orderToken) == "" {
		return fmt.Errorf("rediscache: publish: empty order token")
	}
	if err := s.rdb.Publish(ctx, s.channel(orderToken), strconv.Itoa(int(status))).Err(); err != nil {
		return fmt.Errorf("rediscache: publish: %w", err)
	}
	return nil
}

// Subscribe 订阅指定订单的状态变更流。
// 返回的 Subscription 需要调用方负责 Close 或让 ctx 结束来释放资源。
//
// ctx 的角色：
//   - Subscribe 建立订阅期间用来做连接握手（go-redis 要求一次 Receive 确认）；
//   - forward goroutine 也监听 ctx，所以 ctx 取消后订阅自动结束；
//   - 业务上建议让 ctx 与订阅的生命周期解耦（比如用 context.Background 而不是 request-scoped
//     的 ctx），显式调用 Close 来结束订阅。request-scoped ctx 配合 Close 也能工作，但耦合紧。
func (s *StatusStream) Subscribe(ctx context.Context, orderToken string) (orderflow.Subscription, error) {
	if strings.TrimSpace(orderToken) == "" {
		return nil, fmt.Errorf("rediscache: subscribe: empty order token")
	}

	ps := s.rdb.Subscribe(ctx, s.channel(orderToken))
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return nil, fmt.Errorf("rediscache: subscribe: %w", err)
	}

	sub := &subscription{
		pubsub: ps,
		events: make(chan orderflow.OrderStatus, 8),
		done:   make(chan struct{}),
	}
	go sub.forward(ctx)
	return sub, nil
}

func (s *StatusStream) channel(orderToken string) string {
	return s.keyPrefix + strings.TrimSpace(orderToken)
}

// subscription 是 orderflow.Subscription 的具体实现。
//
// 资源生命周期保证：forward goroutine 的退出由三条路径中任意一条触发：
//  1. ctx 取消（调用方传入的 ctx 结束）；
//  2. done channel 关闭（调用方显式 Close 触发）；
//  3. pubsub 底层 channel 关闭（由 go-redis 在 PubSub.Close 后完成）。
//
// 关键：done channel 让 Close 能立即打断 forward 在 `events <-` 上的阻塞，
// 即便消费端 buffer 被塞满也不会 goroutine 泄漏。
type subscription struct {
	pubsub *redis.PubSub
	events chan orderflow.OrderStatus
	done   chan struct{}
	once   sync.Once
}

func (s *subscription) Events() <-chan orderflow.OrderStatus {
	return s.events
}

// Close 幂等。先 close(done) 让 forward 立刻退出阻塞点，再 close pubsub 解除 msgCh 阻塞。
// 两步顺序不可换——先 close(pubsub) 可能导致 forward 还在 events 写阻塞里时 msgCh 就关了，
// 后续迭代能走 case !ok 分支返回，但中间 done 信号还没有，events 写仍卡着。
func (s *subscription) Close() error {
	var err error
	s.once.Do(func() {
		close(s.done)
		if s.pubsub != nil {
			err = s.pubsub.Close()
		}
	})
	return err
}

// forward 把 Redis Pub/Sub 消息转换为 orderflow.OrderStatus 事件。
// recover 是生产防御：Redis 端格式异常或 go-redis 内部 panic 不应让整进程崩溃。
func (s *subscription) forward(ctx context.Context) {
	defer close(s.events)
	defer func() {
		_ = recover()
	}()

	msgCh := s.pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case msg, ok := <-msgCh:
			if !ok {
				return
			}
			status, err := strconv.Atoi(strings.TrimSpace(msg.Payload))
			if err != nil {
				// 非法 payload 忽略，继续消费下一条。
				continue
			}
			// 两段 select：先非阻塞检查关闭信号（让 done/ctx 优先于 events 发送），
			// 再进入可阻塞写。
			// 未这样做时，3 路 ready（events / done / ctx 都可写）会被 Go select 随机选中，
			// 1/3 概率在 Close 瞬间仍发出一条事件——消费方观察到"Close 后还收到事件"。
			select {
			case <-ctx.Done():
				return
			case <-s.done:
				return
			default:
			}
			select {
			case <-ctx.Done():
				return
			case <-s.done:
				return
			case s.events <- orderflow.OrderStatus(status):
			}
		}
	}
}
