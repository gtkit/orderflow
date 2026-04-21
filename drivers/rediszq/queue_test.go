package rediszq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestScoreForTimeUsesMilliseconds(t *testing.T) {
	fixedTime := time.Unix(1700000000, 456000000)

	if got, want := scoreForTime(fixedTime), float64(fixedTime.UnixMilli()); got != want {
		t.Fatalf("expected millisecond score %v, got %v", want, got)
	}
}

func TestEnqueueRejectsEmptyMember(t *testing.T) {
	queue, err := New(redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"}), "jobs")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = queue.Enqueue(context.Background(), "", time.Now())
	if err == nil {
		t.Fatal("expected enqueue to reject empty member")
	}
}

func TestFetchExpiredRejectsNonPositiveBatchSize(t *testing.T) {
	queue, err := New(redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"}), "jobs")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = queue.FetchExpired(context.Background(), 0)
	if err == nil {
		t.Fatal("expected fetch expired to reject non-positive batch size")
	}
}

func TestNewRejectsInvalidMaxBatch(t *testing.T) {
	_, err := New(redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"}), "jobs", WithMaxBatch(0))
	if err == nil {
		t.Fatal("expected New() to reject non-positive max batch")
	}
}

func TestNewRejectsInvalidDefaultTimeout(t *testing.T) {
	_, err := New(redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"}), "jobs", WithDefaultTimeout(0))
	if err == nil {
		t.Fatal("expected New() to reject non-positive default timeout")
	}
}

func TestReserveExpiredAndAckLifecycle(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t)

	ctx := t.Context()
	if _, err := queue.Enqueue(ctx, "job-1", time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	members, err := queue.ReserveExpired(ctx, 10, 5*time.Second)
	if err != nil {
		t.Fatalf("ReserveExpired() error = %v", err)
	}
	if len(members) != 1 || members[0] != "job-1" {
		t.Fatalf("ReserveExpired() = %v, want [job-1]", members)
	}

	pending, err := queue.Len(ctx)
	if err != nil {
		t.Fatalf("Len() error = %v", err)
	}
	if pending != 0 {
		t.Fatalf("pending len = %d, want 0", pending)
	}

	inflight, err := queue.ProcessingLen(ctx)
	if err != nil {
		t.Fatalf("ProcessingLen() error = %v", err)
	}
	if inflight != 1 {
		t.Fatalf("processing len = %d, want 1", inflight)
	}

	acked, err := queue.Ack(ctx, "job-1")
	if err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if !acked {
		t.Fatal("Ack() = false, want true")
	}

	inflight, err = queue.ProcessingLen(ctx)
	if err != nil {
		t.Fatalf("ProcessingLen() error = %v", err)
	}
	if inflight != 0 {
		t.Fatalf("processing len = %d, want 0 after ack", inflight)
	}
}

func TestRequeueExpiredReturnsUnackedTasks(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t)

	ctx := t.Context()
	if _, err := queue.Enqueue(ctx, "job-1", time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	members, err := queue.ReserveExpired(ctx, 10, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("ReserveExpired() error = %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("ReserveExpired() = %v, want one member", members)
	}

	time.Sleep(30 * time.Millisecond)

	requeued, err := queue.RequeueExpired(ctx, 10)
	if err != nil {
		t.Fatalf("RequeueExpired() error = %v", err)
	}
	if len(requeued) != 1 || requeued[0] != "job-1" {
		t.Fatalf("RequeueExpired() = %v, want [job-1]", requeued)
	}

	members, err = queue.ReserveExpired(ctx, 10, time.Second)
	if err != nil {
		t.Fatalf("ReserveExpired() error = %v", err)
	}
	if len(members) != 1 || members[0] != "job-1" {
		t.Fatalf("ReserveExpired() after requeue = %v, want [job-1]", members)
	}
}

func TestRemoveDeletesPendingAndProcessing(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t)

	ctx := t.Context()
	if _, err := queue.Enqueue(ctx, "job-pending", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if err := queue.Remove(ctx, "job-pending"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}

	pending, err := queue.Len(ctx)
	if err != nil {
		t.Fatalf("Len() error = %v", err)
	}
	if pending != 0 {
		t.Fatalf("pending len = %d, want 0 after pending remove", pending)
	}

	if _, err := queue.Enqueue(ctx, "job-processing", time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if _, err := queue.ReserveExpired(ctx, 10, time.Minute); err != nil {
		t.Fatalf("ReserveExpired() error = %v", err)
	}

	if err := queue.Remove(ctx, "job-processing"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}

	inflight, err := queue.ProcessingLen(ctx)
	if err != nil {
		t.Fatalf("ProcessingLen() error = %v", err)
	}
	if inflight != 0 {
		t.Fatalf("processing len = %d, want 0 after inflight remove", inflight)
	}
}

func TestEnqueueTrimsMember(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t)
	ctx := t.Context()

	if _, err := queue.Enqueue(ctx, "  job-1  ", time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	members, err := queue.FetchExpired(ctx, 10)
	if err != nil {
		t.Fatalf("FetchExpired() error = %v", err)
	}
	if len(members) != 1 || members[0] != "job-1" {
		t.Fatalf("FetchExpired() = %v, want [job-1]", members)
	}
}

func TestWithMaxBatchLimitsReserveSize(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t, WithMaxBatch(2))
	ctx := t.Context()

	for _, member := range []string{"job-1", "job-2", "job-3"} {
		if _, err := queue.Enqueue(ctx, member, time.Now().Add(-time.Second)); err != nil {
			t.Fatalf("Enqueue(%q) error = %v", member, err)
		}
	}

	members, err := queue.ReserveExpired(ctx, 10, time.Second)
	if err != nil {
		t.Fatalf("ReserveExpired() error = %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("ReserveExpired() len = %d, want 2", len(members))
	}
}

func TestExpiredProcessingCountAndStats(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t)
	ctx := t.Context()

	if _, err := queue.Enqueue(ctx, "job-1", time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if _, err := queue.ReserveExpired(ctx, 10, 20*time.Millisecond); err != nil {
		t.Fatalf("ReserveExpired() error = %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	expired, err := queue.ExpiredProcessingCount(ctx)
	if err != nil {
		t.Fatalf("ExpiredProcessingCount() error = %v", err)
	}
	if expired != 1 {
		t.Fatalf("ExpiredProcessingCount() = %d, want 1", expired)
	}

	stats, err := queue.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.QueueName != "jobs" {
		t.Fatalf("Stats().QueueName = %q, want jobs", stats.QueueName)
	}
	if stats.Pending != 0 || stats.Processing != 1 || stats.ExpiredProcessing != 1 {
		t.Fatalf("Stats() = %+v", stats)
	}
}

func TestOperationContextAddsDefaultTimeout(t *testing.T) {
	queue := &Queue{defaultTimeout: time.Second}

	ctx, cancel := queue.operationContext(context.Background())
	defer cancel()

	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("expected operationContext to add deadline")
	}
}

func TestOperationContextPreservesExistingDeadline(t *testing.T) {
	queue := &Queue{defaultTimeout: time.Second}

	parent, cancelParent := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelParent()

	ctx, cancel := queue.operationContext(parent)
	defer cancel()

	parentDeadline, _ := parent.Deadline()
	ctxDeadline, _ := ctx.Deadline()
	if !ctxDeadline.Equal(parentDeadline) {
		t.Fatalf("operationContext changed deadline: got %v want %v", ctxDeadline, parentDeadline)
	}
}

// compileTimeAssertion 验证 *Queue 实现 orderflow.DelayQueue（单独文件避免 import cycle 暴露风险）。
// 这里放在 test 包内，正式使用者通过直接赋值触发接口检查即可。

func newTestQueue(t *testing.T, opts ...Option) *Queue {
	t.Helper()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	queue, err := New(rdb, "jobs", opts...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return queue
}
