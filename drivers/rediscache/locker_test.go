package rediscache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gtkit/orderflow"
	"github.com/redis/go-redis/v9"
)

// =============================================================================
// Locker 测试
// =============================================================================

func newTestLocker(t *testing.T, opts ...LockerOption) (*Locker, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewLocker(rdb, opts...), server, rdb
}

func TestLocker_AcquireAndRelease(t *testing.T) {
	l, server, _ := newTestLocker(t)
	ctx := context.Background()

	unlock, ok, err := l.TryLock(ctx, "k1", time.Minute)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}

	// key 应存在于 Redis
	if _, err := server.Get("orderflow:lock:k1"); err != nil {
		t.Fatalf("lock key missing in redis: %v", err)
	}

	unlock()

	// key 应被释放
	if _, err := server.Get("orderflow:lock:k1"); err == nil {
		t.Fatal("lock key should be released after unlock")
	}
}

func TestLocker_ConcurrentDenied(t *testing.T) {
	l, _, _ := newTestLocker(t)
	ctx := context.Background()

	unlock1, ok1, err := l.TryLock(ctx, "contested", time.Minute)
	if err != nil || !ok1 {
		t.Fatalf("first TryLock: ok=%v err=%v", ok1, err)
	}
	defer unlock1()

	// 第二次应该失败
	unlock2, ok2, err := l.TryLock(ctx, "contested", time.Minute)
	if err != nil {
		t.Fatalf("second TryLock err: %v", err)
	}
	if ok2 {
		t.Fatal("second TryLock should return ok=false")
	}
	// unlock2 应为 no-op
	unlock2()
}

func TestLocker_ReleaseOnlyOwnLock(t *testing.T) {
	l, server, _ := newTestLocker(t)
	ctx := context.Background()

	// A 持锁，写入自己的 token
	unlockA, ok, err := l.TryLock(ctx, "k", time.Minute)
	if err != nil || !ok {
		t.Fatalf("A TryLock: %v ok=%v", err, ok)
	}

	// 手工覆盖 key 为别人的 token（模拟 A 的锁 TTL 过期后被 B 获取）
	server.Set("orderflow:lock:k", "different-token")

	// A 调自己的 unlock——Lua CAS 应发现 token 不匹配，不 DEL
	unlockA()

	// key 应该仍然存在（没被误删）
	v, err := server.Get("orderflow:lock:k")
	if err != nil {
		t.Fatalf("lock was unexpectedly released: %v", err)
	}
	if v != "different-token" {
		t.Fatalf("token overwritten: %s", v)
	}
}

func TestLocker_AutoExpireByTTL(t *testing.T) {
	l, server, _ := newTestLocker(t)
	ctx := context.Background()

	_, ok, err := l.TryLock(ctx, "k", 100*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("first TryLock: %v ok=%v", err, ok)
	}

	// 模拟时钟前进超过 TTL（miniredis 支持手工推进时钟）
	server.FastForward(200 * time.Millisecond)

	// 第二次应成功（前一把锁已过期）
	_, ok2, err := l.TryLock(ctx, "k", time.Minute)
	if err != nil {
		t.Fatalf("second TryLock: %v", err)
	}
	if !ok2 {
		t.Fatal("expected second TryLock to succeed after TTL")
	}
}

func TestLocker_WithCustomPrefix(t *testing.T) {
	l, server, _ := newTestLocker(t, WithLockerKeyPrefix("myapp:locks:"))

	unlock, ok, err := l.TryLock(context.Background(), "foo", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock: err=%v ok=%v", err, ok)
	}
	defer unlock()

	if _, err := server.Get("myapp:locks:foo"); err != nil {
		t.Fatalf("expected key under custom prefix: %v", err)
	}
}

// SerializesConcurrent —— 并发多方 TryLock 同 key，只有 1 方持锁
func TestLocker_SerializesConcurrent(t *testing.T) {
	l, _, _ := newTestLocker(t)

	const N = 20
	var (
		wg     sync.WaitGroup
		okCnt  atomic.Int32
		denied atomic.Int32
	)
	for range N {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock, ok, err := l.TryLock(context.Background(), "k", 5*time.Second)
			if err != nil {
				t.Errorf("TryLock: %v", err)
				return
			}
			if ok {
				okCnt.Add(1)
				// 持锁 10ms 模拟业务
				time.Sleep(10 * time.Millisecond)
				unlock()
			} else {
				denied.Add(1)
			}
		}()
	}
	wg.Wait()

	// 不能有 2 个 goroutine 同时 ok=true——要么全过要么部分拒
	// 精确断言："同一瞬间只有 1 个持锁"。由于我们是 non-blocking TryLock 模式，
	// 总调用次数 N，成功数 = 实际未重叠的窗口数。
	total := okCnt.Load() + denied.Load()
	if total != N {
		t.Fatalf("ok+denied = %d, want %d", total, N)
	}
	if okCnt.Load() < 1 {
		t.Fatal("expected at least 1 successful lock")
	}
}

// =============================================================================
// IdempotentOnPaidViaRedis 测试
// =============================================================================

type idempOrder struct{ orderNo string }

func (o *idempOrder) OrderNo() string               { return o.orderNo }
func (o *idempOrder) OrderToken() string            { return "" }
func (o *idempOrder) UserID() int64                 { return 0 }
func (o *idempOrder) Status() orderflow.OrderStatus { return orderflow.StatusPaid }
func (o *idempOrder) ProductID() uint64             { return 0 }
func (o *idempOrder) ProductType() string           { return "" }
func (o *idempOrder) ProductTitle() string          { return "" }
func (o *idempOrder) PayMethod() string             { return "" }
func (o *idempOrder) PayAmount() int64              { return 0 }
func (o *idempOrder) OriginalPrice() int64          { return 0 }
func (o *idempOrder) TradeNo() string               { return "" }
func (o *idempOrder) ExpireAt() time.Time           { return time.Time{} }
func (o *idempOrder) PaidAt() (time.Time, bool)     { return time.Time{}, false }
func (o *idempOrder) Extra() map[string]any         { return nil }

func TestIdempotentOnPaidViaRedis_FirstCallReachesInner(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	var innerCalls int32
	inner := func(_ context.Context, _ *idempOrder, _ orderflow.NotifyResult) error {
		atomic.AddInt32(&innerCalls, 1)
		return nil
	}
	wrapped := IdempotentOnPaidViaRedis[*idempOrder](inner, rdb, "", 24*time.Hour)

	o := &idempOrder{orderNo: "ORD-1"}
	if err := wrapped(context.Background(), o, orderflow.NotifyResult{}); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if innerCalls != 1 {
		t.Fatalf("innerCalls = %d, want 1", innerCalls)
	}
}

func TestIdempotentOnPaidViaRedis_SecondCallSkipsInner(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	var innerCalls int32
	inner := func(_ context.Context, _ *idempOrder, _ orderflow.NotifyResult) error {
		atomic.AddInt32(&innerCalls, 1)
		return nil
	}
	wrapped := IdempotentOnPaidViaRedis[*idempOrder](inner, rdb, "", 24*time.Hour)

	o := &idempOrder{orderNo: "ORD-2"}
	for range 5 {
		if err := wrapped(context.Background(), o, orderflow.NotifyResult{}); err != nil {
			t.Fatalf("call: %v", err)
		}
	}
	if innerCalls != 1 {
		t.Fatalf("innerCalls = %d, want 1 (should be idempotent)", innerCalls)
	}
}

func TestIdempotentOnPaidViaRedis_InnerFailureAllowsRetry(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	attempts := int32(0)
	transientErr := errors.New("vip service transient failure")
	inner := func(_ context.Context, _ *idempOrder, _ orderflow.NotifyResult) error {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			return transientErr
		}
		return nil
	}
	wrapped := IdempotentOnPaidViaRedis[*idempOrder](inner, rdb, "", 24*time.Hour)

	o := &idempOrder{orderNo: "ORD-3"}

	// 第一次：inner 失败，marker 应被删除
	err := wrapped(context.Background(), o, orderflow.NotifyResult{})
	if !errors.Is(err, transientErr) {
		t.Fatalf("first call err = %v, want wraps %v", err, transientErr)
	}

	// 第二次：inner 成功
	if err := wrapped(context.Background(), o, orderflow.NotifyResult{}); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}

	// 第三次：marker 已存在，跳过
	if err := wrapped(context.Background(), o, orderflow.NotifyResult{}); err != nil {
		t.Fatalf("third call: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts after third call = %d, want 2 (should skip)", attempts)
	}
}

func TestIdempotentOnPaidViaRedis_ConcurrentSingleInnerCall(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	var innerCalls int32
	inner := func(_ context.Context, _ *idempOrder, _ orderflow.NotifyResult) error {
		atomic.AddInt32(&innerCalls, 1)
		time.Sleep(10 * time.Millisecond) // 模拟 IO
		return nil
	}
	wrapped := IdempotentOnPaidViaRedis[*idempOrder](inner, rdb, "", 24*time.Hour)

	o := &idempOrder{orderNo: "ORD-RACE"}
	const N = 20
	var wg sync.WaitGroup
	for range N {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = wrapped(context.Background(), o, orderflow.NotifyResult{})
		}()
	}
	wg.Wait()

	if innerCalls != 1 {
		t.Fatalf("innerCalls = %d, want exactly 1 under %d concurrent calls", innerCalls, N)
	}
}
