package orderflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// fakeLocker —— 内存 Locker，支持错误注入
// =============================================================================

type fakeLocker struct {
	mu     sync.Mutex
	locked map[string]bool

	// TryLockErr 注入 TryLock 返回错误。
	TryLockErr error

	// DenyAll 强制所有 TryLock 返回 ok=false（模拟被占）。
	DenyAll bool

	TryLockCalls int32
	UnlockCalls  int32
}

func newFakeLocker() *fakeLocker {
	return &fakeLocker{locked: make(map[string]bool)}
}

func (l *fakeLocker) TryLock(_ context.Context, key string, _ time.Duration) (func(), bool, error) {
	atomic.AddInt32(&l.TryLockCalls, 1)
	if l.TryLockErr != nil {
		return func() {}, false, l.TryLockErr
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.DenyAll || l.locked[key] {
		return func() {}, false, nil
	}
	l.locked[key] = true
	unlock := func() {
		atomic.AddInt32(&l.UnlockCalls, 1)
		l.mu.Lock()
		defer l.mu.Unlock()
		delete(l.locked, key)
	}
	return unlock, true, nil
}

// =============================================================================
// Engine.Create 集成 Locker 的测试
// =============================================================================

func TestCreate_WithLocker_HappyPath(t *testing.T) {
	env := newTestEnv(t)
	lock := newFakeLocker()
	env.engine.locker = lock
	env.engine.createLockTTL = 10 * time.Second

	result, err := env.engine.Create(context.Background(), standardRequest())
	mustNotErr(t, err, "Create")
	if result.Order == nil {
		t.Fatal("no order returned")
	}

	// TryLock 被调 1 次，Unlock 被调 1 次
	mustEqual(t, atomic.LoadInt32(&lock.TryLockCalls), int32(1), "TryLockCalls")
	mustEqual(t, atomic.LoadInt32(&lock.UnlockCalls), int32(1), "UnlockCalls")
}

func TestCreate_WithLocker_ConcurrentConflictReturnsErrConcurrentCreate(t *testing.T) {
	env := newTestEnv(t)
	lock := newFakeLocker()
	lock.DenyAll = true // 模拟别的 goroutine 已持锁
	env.engine.locker = lock

	// 注入 recordingObserver 校验并发拒绝会发出 EventAnomaly
	rec := newRecordingObserver()
	env.engine.observer = rec

	_, err := env.engine.Create(context.Background(), standardRequest())
	if !errors.Is(err, ErrConcurrentCreate) {
		t.Fatalf("expected ErrConcurrentCreate, got %v", err)
	}

	// 没有订单被创建
	mustEqual(t, env.store.CreateCalls, 0, "no Store.Create")

	// EventAnomaly 发出，kind=concurrent_create_rejected
	anomalies := rec.byKind(EventAnomaly)
	if len(anomalies) != 1 {
		t.Fatalf("want 1 anomaly event, got %d", len(anomalies))
	}
	if got := anomalies[0].Attrs["kind"]; got != "concurrent_create_rejected" {
		t.Errorf("anomaly kind = %v, want concurrent_create_rejected", got)
	}
}

func TestCreate_WithLocker_LockerErrorPropagates(t *testing.T) {
	env := newTestEnv(t)
	lock := newFakeLocker()
	lock.TryLockErr = errors.New("redis down")
	env.engine.locker = lock

	_, err := env.engine.Create(context.Background(), standardRequest())
	if err == nil {
		t.Fatal("expected error when locker fails")
	}
	if !errContains(err, "acquire create lock") {
		t.Errorf("err = %v, want wrap of 'acquire create lock'", err)
	}
	// 不应创建订单
	mustEqual(t, env.store.CreateCalls, 0, "no Store.Create")
}

func TestCreate_WithLocker_SerializesSameUserProduct(t *testing.T) {
	// 真正的串行化测试：50 并发 Create 同用户同商品，带 Locker 后
	// 应该只有 1 个能通过（首次 Create 成功），其他 49 个得到 ErrConcurrentCreate。
	env := newTestEnv(t)
	lock := newFakeLocker()
	env.engine.locker = lock

	const N = 50
	var (
		wg       sync.WaitGroup
		ok       atomic.Int32
		conflict atomic.Int32
		other    atomic.Int32
	)
	for range N {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := env.engine.Create(context.Background(), standardRequest())
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrConcurrentCreate):
				conflict.Add(1)
			default:
				other.Add(1)
			}
		}()
	}
	wg.Wait()

	t.Logf("ok=%d conflict=%d other=%d", ok.Load(), conflict.Load(), other.Load())
	if other.Load() != 0 {
		t.Fatalf("unexpected errors: %d", other.Load())
	}
	if ok.Load()+conflict.Load() != N {
		t.Fatalf("totals don't add up: ok=%d + conflict=%d != %d", ok.Load(), conflict.Load(), N)
	}
	// 至少 1 次成功（fakeLocker 串行化所以不会全部 conflict）
	if ok.Load() < 1 {
		t.Errorf("expected at least 1 successful Create, got 0")
	}
	// 最终状态：store 里对同用户同商品至多 1 个 Pending
	pendingForUserProduct := 0
	for _, o := range env.store.byNo {
		if o.userID == 1001 && o.productID == 2001 && o.status == StatusPending {
			pendingForUserProduct++
		}
	}
	if pendingForUserProduct > 1 {
		t.Errorf("multiple Pending orders for same (user, product): %d", pendingForUserProduct)
	}
}

func TestCreate_WithoutLocker_KeepsLegacyBehavior(t *testing.T) {
	// 未注入 Locker 时行为不变（v0.5.3 混沌测试记录的"限制"依然存在——无强制约束）
	env := newTestEnv(t)
	// 不设 env.engine.locker

	_, err := env.engine.Create(context.Background(), standardRequest())
	mustNotErr(t, err, "Create without locker")
}

// =============================================================================
// IdempotentOnPaid helper 测试
// =============================================================================

func TestIdempotentOnPaid_SkipsWhenMarkerExists(t *testing.T) {
	var innerCalls int32
	inner := func(_ context.Context, _ *testOrder, _ NotifyResult) error {
		atomic.AddInt32(&innerCalls, 1)
		return nil
	}
	markerExists := func(_ context.Context, _ string) (bool, error) {
		return true, nil // 总是说 marker 已存在
	}

	wrapped := IdempotentOnPaid[*testOrder](inner, markerExists)
	order := &testOrder{orderNo: "N1"}

	// 调 3 次，inner 都不应被调
	for range 3 {
		mustNotErr(t, wrapped(context.Background(), order, NotifyResult{}), "wrapped")
	}

	if innerCalls != 0 {
		t.Errorf("inner called %d times, want 0", innerCalls)
	}
}

func TestIdempotentOnPaid_CallsInnerWhenMarkerAbsent(t *testing.T) {
	var innerCalls int32
	inner := func(_ context.Context, _ *testOrder, _ NotifyResult) error {
		atomic.AddInt32(&innerCalls, 1)
		return nil
	}
	markerExists := func(_ context.Context, _ string) (bool, error) {
		return false, nil
	}

	wrapped := IdempotentOnPaid[*testOrder](inner, markerExists)
	order := &testOrder{orderNo: "N2"}
	mustNotErr(t, wrapped(context.Background(), order, NotifyResult{}), "wrapped")

	if innerCalls != 1 {
		t.Errorf("inner called %d times, want 1", innerCalls)
	}
}

func TestIdempotentOnPaid_MarkerExistsErrorPropagates(t *testing.T) {
	innerCalled := false
	inner := func(_ context.Context, _ *testOrder, _ NotifyResult) error {
		innerCalled = true
		return nil
	}
	checkErr := errors.New("db down")
	markerExists := func(_ context.Context, _ string) (bool, error) {
		return false, checkErr
	}

	wrapped := IdempotentOnPaid[*testOrder](inner, markerExists)
	err := wrapped(context.Background(), &testOrder{orderNo: "N3"}, NotifyResult{})
	if !errors.Is(err, checkErr) {
		t.Fatalf("err = %v, want wraps %v", err, checkErr)
	}
	if innerCalled {
		t.Error("inner should not be called when marker check fails")
	}
}

func TestIdempotentOnPaid_InnerErrorPropagates(t *testing.T) {
	innerErr := errors.New("vip service timeout")
	inner := func(_ context.Context, _ *testOrder, _ NotifyResult) error {
		return innerErr
	}
	markerExists := func(_ context.Context, _ string) (bool, error) {
		return false, nil
	}

	wrapped := IdempotentOnPaid[*testOrder](inner, markerExists)
	err := wrapped(context.Background(), &testOrder{orderNo: "N4"}, NotifyResult{})
	if !errors.Is(err, innerErr) {
		t.Fatalf("err = %v, want wraps %v", err, innerErr)
	}
}

func TestIdempotentOnPaid_NilInnerReturnsError(t *testing.T) {
	markerExists := func(_ context.Context, _ string) (bool, error) {
		return false, nil
	}
	wrapped := IdempotentOnPaid[*testOrder](nil, markerExists)

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("unexpected panic: %v", r)
			}
		}()
		err = wrapped(context.Background(), &testOrder{orderNo: "N5"}, NotifyResult{})
	}()
	if err == nil {
		t.Fatal("expected explicit error for nil inner hook")
	}
}

func TestIdempotentOnPaid_NilMarkerExistsReturnsError(t *testing.T) {
	inner := func(_ context.Context, _ *testOrder, _ NotifyResult) error {
		return nil
	}
	wrapped := IdempotentOnPaid[*testOrder](inner, nil)

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("unexpected panic: %v", r)
			}
		}()
		err = wrapped(context.Background(), &testOrder{orderNo: "N6"}, NotifyResult{})
	}()
	if err == nil {
		t.Fatal("expected explicit error for nil markerExists")
	}
}

// 辅助：errContains 检查 err.Error() 包含子串（不使用 fmt.Sprintf 避免意外 nil panic）
func errContains(err error, sub string) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
