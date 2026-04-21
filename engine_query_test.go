package orderflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

// PollStatus 的交叉验证点：
//   - 缓存命中：直接返回，不访问 Store
//   - 缓存归属校验：UserID 不匹配返回 ErrOrderForbidden
//   - 缓存 miss：回源 DB，回填缓存
//   - 不存在：ErrOrderNotFound

func TestPollStatus_CacheHit(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// 预置缓存：不 seed store，验证命中 cache 时根本不查库
	const token = "TOKEN-A"
	mustNotErr(t, env.cache.Set(ctx, token, 1001, StatusPaid, time.Time{}), "cache Set")
	env.cache.SetCalls = 0 // 重置计数，仅统计 Poll 内部行为

	result, err := env.engine.PollStatus(ctx, token, 1001)
	mustNotErr(t, err, "PollStatus")
	mustEqual(t, result.Status, StatusPaid, "Status")
	mustEqual(t, result.StatusText, "paid", "StatusText")

	// 交叉验证：Cache.GetCalls == 1，未回源 Store，未再次 Set
	mustEqual(t, env.cache.GetCalls, 1, "Cache.GetCalls")
	mustEqual(t, env.cache.SetCalls, 0, "Cache.SetCalls (should not backfill on hit)")
}

func TestPollStatus_CacheForbidden(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const token = "TOKEN-FORBIDDEN"
	mustNotErr(t, env.cache.Set(ctx, token, 1001, StatusPaid, time.Time{}), "cache Set")

	_, err := env.engine.PollStatus(ctx, token, 9999)
	if !errors.Is(err, ErrOrderForbidden) {
		t.Fatalf("expected ErrOrderForbidden, got %v", err)
	}
}

func TestPollStatus_CacheMissFallsBackToDB(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const token = "TOKEN-DB"
	// 只 seed Store，不写 cache
	o := &testOrder{
		orderNo:    "NO-1",
		orderToken: token,
		userID:     1001,
		status:     StatusDelivered,
		expireAt:   time.Now().Add(time.Hour),
	}
	env.store.seed(o)

	result, err := env.engine.PollStatus(ctx, token, 1001)
	mustNotErr(t, err, "PollStatus")
	mustEqual(t, result.Status, StatusDelivered, "Status")

	// 交叉验证：cache Get 一次 miss，然后回填写入
	mustEqual(t, env.cache.GetCalls, 1, "Cache.GetCalls")
	mustEqual(t, env.cache.SetCalls, 1, "Cache.SetCalls (backfill after db fallback)")
	// 再查一次，应该走 cache
	_, _ = env.engine.PollStatus(ctx, token, 1001)
	mustEqual(t, env.cache.GetCalls, 2, "Cache.GetCalls (2nd)")
	mustEqual(t, env.cache.SetCalls, 1, "Cache.SetCalls (no new backfill)")
}

func TestPollStatus_NotFound(t *testing.T) {
	env := newTestEnv(t)
	_, err := env.engine.PollStatus(context.Background(), "UNKNOWN", 1001)
	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("expected ErrOrderNotFound, got %v", err)
	}
}

func TestPollStatus_DBForbidden(t *testing.T) {
	env := newTestEnv(t)
	const token = "TOKEN-WRONG-USER"
	env.store.seed(&testOrder{
		orderNo:    "NO-2",
		orderToken: token,
		userID:     1001,
		status:     StatusPending,
	})

	_, err := env.engine.PollStatus(context.Background(), token, 9999)
	if !errors.Is(err, ErrOrderForbidden) {
		t.Fatalf("expected ErrOrderForbidden, got %v", err)
	}
}

// Timeline：既要校验权限（同 PollStatus），也要验证日志透传
func TestTimeline_HappyPath(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const token = "TOKEN-TL"
	env.store.seed(&testOrder{
		orderNo:    "NO-TL",
		orderToken: token,
		userID:     1001,
		status:     StatusDelivered,
	})
	mustNotErr(t, env.store.AppendLog(ctx, LogEntry{
		OrderNo: "NO-TL", FromStatus: StatusPending, ToStatus: StatusPaid,
		Actor: "system", Remark: "paid", CreatedAt: time.Now(),
	}), "seed log")
	mustNotErr(t, env.store.AppendLog(ctx, LogEntry{
		OrderNo: "NO-TL", FromStatus: StatusPaid, ToStatus: StatusDelivered,
		Actor: "system", Remark: "delivered", CreatedAt: time.Now(),
	}), "seed log")

	tl, err := env.engine.Timeline(ctx, token, 1001)
	mustNotErr(t, err, "Timeline")
	mustEqual(t, tl.OrderNo, "NO-TL", "OrderNo")
	mustEqual(t, tl.Status, StatusDelivered, "Status")
	mustLen(t, tl.Entries, 2, "Entries")
}

func TestTimeline_Forbidden(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{orderNo: "NO", orderToken: "T", userID: 1001, status: StatusPending})

	_, err := env.engine.Timeline(context.Background(), "T", 9999)
	if !errors.Is(err, ErrOrderForbidden) {
		t.Fatalf("expected ErrOrderForbidden, got %v", err)
	}
}

func TestTimeline_NotFound(t *testing.T) {
	env := newTestEnv(t)
	_, err := env.engine.Timeline(context.Background(), "UNKNOWN", 1001)
	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("expected ErrOrderNotFound, got %v", err)
	}
}

func TestListUserOrders_FiltersByUser(t *testing.T) {
	env := newTestEnv(t)
	for i, userID := range []int64{1001, 1001, 1002, 1001} {
		env.store.seed(&testOrder{
			orderNo:    string(rune('A' + i)),
			orderToken: "t" + string(rune('A'+i)),
			userID:     userID,
			status:     StatusPending,
		})
	}

	got, err := env.engine.ListUserOrders(context.Background(), 1001)
	mustNotErr(t, err, "ListUserOrders")
	mustLen(t, got, 3, "1001 orders")

	got, err = env.engine.ListUserOrders(context.Background(), 1002)
	mustNotErr(t, err, "ListUserOrders")
	mustLen(t, got, 1, "1002 orders")

	got, err = env.engine.ListUserOrders(context.Background(), 9999)
	mustNotErr(t, err, "ListUserOrders")
	mustLen(t, got, 0, "empty user")
}
