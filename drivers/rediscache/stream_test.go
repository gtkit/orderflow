package rediscache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gtkit/orderflow"
	"github.com/redis/go-redis/v9"
)

func newTestStream(t *testing.T, opts ...StreamOption) (*StatusStream, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewStatusStream(rdb, opts...), server, rdb
}

func TestStream_PublishSubscribeRoundtrip(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestStream(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sub, err := s.Subscribe(ctx, "TOK-S")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	// 发 3 个事件
	want := []orderflow.OrderStatus{
		orderflow.StatusPaid,
		orderflow.StatusDelivered,
		orderflow.StatusCompleted,
	}
	go func() {
		for _, st := range want {
			if err := s.Publish(ctx, "TOK-S", st); err != nil {
				t.Errorf("Publish %v: %v", st, err)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// 收 3 个
	for i, expected := range want {
		select {
		case got, ok := <-sub.Events():
			if !ok {
				t.Fatalf("events chan closed early at #%d", i)
			}
			if got != expected {
				t.Errorf("event #%d: got %v, want %v", i, got, expected)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for event #%d", i)
		}
	}
}

func TestStream_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestStream(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub, err := s.Subscribe(ctx, "TOK-C")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// 连续 Close 多次不应 panic 或返回不同 error
	if err := sub.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}

	// Events chan 应该被关闭（forward goroutine 退出）
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Error("expected closed chan after Close")
		}
	case <-time.After(500 * time.Millisecond):
		// 即使 chan 没立即关闭也允许（取决于 forward goroutine 调度），但至少不死锁
	}
}

func TestStream_PublishRejectsEmptyToken(t *testing.T) {
	s, _, _ := newTestStream(t)
	err := s.Publish(context.Background(), "", orderflow.StatusPaid)
	if err == nil {
		t.Fatal("expected error on empty token")
	}
}

func TestStream_SubscribeRejectsEmptyToken(t *testing.T) {
	s, _, _ := newTestStream(t)
	_, err := s.Subscribe(context.Background(), "   ")
	if err == nil {
		t.Fatal("expected error on empty token")
	}
}

func TestStream_CustomPrefix(t *testing.T) {
	s, _, _ := newTestStream(t, WithStreamKeyPrefix("myapp:events:"))
	got := s.channel("TOK")
	if got != "myapp:events:TOK" {
		t.Errorf("channel = %q, want myapp:events:TOK", got)
	}
}

// 专项：验证 C2 修复——消费端不读 + Close 调用时 forward goroutine 能立即退出。
// 回归保证：未修前 forward 会永远卡在 `events <-` 阻塞，进程 goroutine 泄漏。
func TestStream_CloseUnblocksForwardWhenBufferFull(t *testing.T) {
	t.Parallel()
	s, server, _ := newTestStream(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sub, err := s.Subscribe(ctx, "TOK-SLOW")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// 发送 events 缓冲大小（8）+ 额外几条，确保 forward 在内层 `events <-` 阻塞
	go func() {
		for range 12 {
			server.Publish("orderflow:status:events:TOK-SLOW", "10")
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(100 * time.Millisecond) // 给 forward 时间塞满 buffer 并卡住

	// 消费端一条都没读，直接 Close
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- sub.Close()
	}()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s — forward goroutine leaked on full buffer")
	}

	// Close 返回后 events chan 应该关闭（forward 已退出）
	select {
	case _, ok := <-sub.Events():
		if ok {
			// 剩余 buffer 里的事件可以继续读出来，这是正常的；
			// 继续读到 chan close 信号才算 forward 真的退出
			for {
				if _, stillOpen := <-sub.Events(); !stillOpen {
					return
				}
			}
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("events chan not closed within 500ms after Close")
	}
}

// Forward goroutine 对非法 payload 的处理：忽略并继续
func TestStream_IgnoresMalformedPayload(t *testing.T) {
	t.Parallel()
	s, server, _ := newTestStream(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sub, err := s.Subscribe(ctx, "TOK-M")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	go func() {
		// 发一条垃圾 + 一条合法
		server.Publish("orderflow:status:events:TOK-M", "not-a-number")
		time.Sleep(20 * time.Millisecond)
		server.Publish("orderflow:status:events:TOK-M", "10") // StatusPaid
	}()

	select {
	case got, ok := <-sub.Events():
		if !ok {
			t.Fatal("events chan closed before valid message")
		}
		if got != orderflow.StatusPaid {
			t.Errorf("got %v, want StatusPaid", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for valid event (malformed should not block)")
	}
}

func TestStream_NilClientReturnsError(t *testing.T) {
	s := NewStatusStream(nil)

	t.Run("Publish", func(t *testing.T) {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("unexpected panic: %v", r)
				}
			}()
			err = s.Publish(context.Background(), "TOK", orderflow.StatusPaid)
		}()
		if err == nil {
			t.Fatal("expected error for nil redis client")
		}
	})

	t.Run("Subscribe", func(t *testing.T) {
		var (
			sub orderflow.Subscription
			err error
		)
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("unexpected panic: %v", r)
				}
			}()
			sub, err = s.Subscribe(context.Background(), "TOK")
		}()
		if sub != nil {
			t.Fatalf("expected nil subscription, got %#v", sub)
		}
		if err == nil {
			t.Fatal("expected error for nil redis client")
		}
	})
}

// recordingLogger 实现 orderflow.Logger 接口，把每次 Error 调用记入计数 + 缓冲区。
type recordingLogger struct {
	errCalls int
	lastMsg  string
}

func (l *recordingLogger) Debug(context.Context, string, ...orderflow.Field) {}
func (l *recordingLogger) Info(context.Context, string, ...orderflow.Field)  {}
func (l *recordingLogger) Warn(context.Context, string, ...orderflow.Field)  {}
func (l *recordingLogger) Error(_ context.Context, msg string, _ ...orderflow.Field) {
	l.errCalls++
	l.lastMsg = msg
}

func TestStream_ForwardLogsRecoveredPanic(t *testing.T) {
	rec := &recordingLogger{}
	sub := &subscription{
		logger: rec,
		done:   make(chan struct{}),
		events: make(chan orderflow.OrderStatus, 1),
	}

	sub.forward(context.Background())

	if rec.errCalls == 0 {
		t.Fatal("expected recovered panic to be logged via Logger.Error")
	}
}
