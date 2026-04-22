package rediszq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// 这一组测试覆盖 queue.go 中的参数校验 / 错误分支：
//  - Option 构造器对非法值的拒绝
//  - New 对 nil rdb / empty key / opt 返回 error 的拒绝
//  - MustNew 的 panic 路径
//  - 各公开方法对 nil queue / empty member / 非正 batch 的拒绝
// 目的是把覆盖率从 73.7% 拉到 >= 80%，同时把防御性代码也锁进回归套件。

func TestWithDefaultTimeoutRejectsNonPositive(t *testing.T) {
	cases := []time.Duration{0, -time.Second}
	for _, d := range cases {
		opt := WithDefaultTimeout(d)
		var q Queue
		if err := opt(&q); err == nil {
			t.Errorf("expected error for timeout %v", d)
		}
	}
}

func TestNewRejectsNilRedisClient(t *testing.T) {
	_, err := New(nil, "jobs")
	if err == nil {
		t.Fatal("expected error for nil redis client")
	}
}

func TestNewRejectsEmptyKey(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	for _, k := range []string{"", "   ", "\t"} {
		if _, err := New(rdb, k); err == nil {
			t.Errorf("expected error for key %q", k)
		}
	}
}

func TestNewSkipsNilOption(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	// 显式传入 nil option，构造仍应成功（guard 分支）
	if _, err := New(rdb, "jobs", nil, WithMaxBatch(10), nil); err != nil {
		t.Fatalf("nil option should be skipped silently, got %v", err)
	}
}

func TestNewPropagatesOptionError(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	_, err := New(rdb, "jobs", WithMaxBatch(-1))
	if err == nil {
		t.Fatal("expected New to propagate option error")
	}
}

func TestMustNewPanicsOnError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected MustNew to panic on invalid args")
		}
	}()
	_ = MustNew(nil, "jobs")
}

func TestMustNewReturnsQueueOnSuccess(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	if q := MustNew(rdb, "jobs"); q == nil {
		t.Fatal("MustNew should return non-nil queue on success")
	}
}

// ----- 方法对 nil queue 的 guard -----

func TestMethodsOnNilQueueReturnError(t *testing.T) {
	var q *Queue // nil

	ctx := context.Background()

	if _, err := q.Enqueue(ctx, "m", time.Now()); err == nil {
		t.Error("Enqueue on nil queue should error")
	}
	if err := q.Remove(ctx, "m"); err == nil {
		t.Error("Remove on nil queue should error")
	}
	if _, err := q.FetchExpired(ctx, 1); err == nil {
		t.Error("FetchExpired on nil queue should error")
	}
	if _, err := q.ReserveExpired(ctx, 1, time.Second); err == nil {
		t.Error("ReserveExpired on nil queue should error")
	}
	if _, err := q.Ack(ctx, "m"); err == nil {
		t.Error("Ack on nil queue should error")
	}
	if _, err := q.RequeueExpired(ctx, 1); err == nil {
		t.Error("RequeueExpired on nil queue should error")
	}
	if _, err := q.Len(ctx); err == nil {
		t.Error("Len on nil queue should error")
	}
	if _, err := q.ProcessingLen(ctx); err == nil {
		t.Error("ProcessingLen on nil queue should error")
	}
	if _, err := q.ExpiredProcessingCount(ctx); err == nil {
		t.Error("ExpiredProcessingCount on nil queue should error")
	}
	if _, err := q.Stats(ctx); err == nil {
		t.Error("Stats on nil queue should error")
	}
}

// ----- 成员 / batch 参数校验 -----

func TestMemberArgsRejectEmpty(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	q, err := New(rdb, "jobs")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if err := q.Remove(ctx, "   "); err == nil {
		t.Error("Remove should reject whitespace member")
	}
	if _, err := q.Ack(ctx, ""); err == nil {
		t.Error("Ack should reject empty member")
	}
}

func TestBatchParamsReject(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	q, err := New(rdb, "jobs")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if _, err := q.ReserveExpired(ctx, 0, time.Second); err == nil {
		t.Error("ReserveExpired should reject batchSize <= 0")
	}
	if _, err := q.ReserveExpired(ctx, 1, 0); err == nil {
		t.Error("ReserveExpired should reject lease <= 0")
	}
	if _, err := q.RequeueExpired(ctx, -1); err == nil {
		t.Error("RequeueExpired should reject batchSize <= 0")
	}
}

// ----- 成功路径 Stats / Len / ProcessingLen 等覆盖 -----

func TestLenAndProcessingLenAgainstMiniredis(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	q, err := New(rdb, "jobs")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if n, err := q.Len(ctx); err != nil || n != 0 {
		t.Fatalf("Len initial: n=%d err=%v", n, err)
	}
	if _, err := q.Enqueue(ctx, "m1", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.Enqueue(ctx, "m2", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	n, err := q.Len(ctx)
	if err != nil {
		t.Fatalf("Len: %v", err)
	}
	if n != 2 {
		t.Errorf("Len = %d, want 2", n)
	}

	// ProcessingLen 应该为 0（没 Reserve 过）
	if pn, err := q.ProcessingLen(ctx); err != nil || pn != 0 {
		t.Fatalf("ProcessingLen: n=%d err=%v", pn, err)
	}

	// ExpiredProcessingCount 在 miniredis 下对空 processing 集合应返回 0
	if exp, err := q.ExpiredProcessingCount(ctx); err != nil || exp != 0 {
		t.Fatalf("ExpiredProcessingCount: n=%d err=%v", exp, err)
	}
}

// Stats 在真实 miniredis 下同时调用 Len / ProcessingLen / ExpiredProcessingCount 聚合
func TestStatsAggregate(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	q, err := New(rdb, "jobs")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, "m1", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.QueueName != "jobs" || stats.Pending != 1 || stats.Processing != 0 {
		t.Errorf("Stats unexpected: %+v", stats)
	}
}

// ----- Enqueue 传入仅空白的 member 应被 normalizeMember 拒绝 -----

func TestEnqueueRejectsWhitespaceMember(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	q, err := New(rdb, "jobs")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := q.Enqueue(context.Background(), "  \t ", time.Now()); err == nil {
		t.Error("Enqueue should reject whitespace-only member")
	}
}

// ----- 通过 miniredis 复现 Redis 错误路径（例如 cluster down）-----

func TestEnqueueSurfacesRedisError(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	q, err := New(rdb, "jobs")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 用已关闭的 server 触发真实 rdb 错误
	server.Close()
	_, err = q.Enqueue(context.Background(), "m", time.Now())
	if err == nil || errors.Is(err, context.Canceled) {
		t.Errorf("expected enqueue to surface redis error, got %v", err)
	}
}
