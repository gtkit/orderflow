package worker

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderflow"
)

// close_worker 主要测 poll 循环：从 DelayQueue 拿到期任务 → 调 Engine.Close → Ack。
// 交叉验证：store 里对应订单状态变成 Closed + queue.acked 里有该 member + queue.pending 清空。

func TestCloseWorker_PollConsumesExpiredAndAcks(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	now := time.Now()

	// 播种 3 个已过期的 Pending
	for _, n := range []string{"A", "B", "C"} {
		rig.store.seed(&testOrder{
			orderNo:    n,
			orderToken: "t-" + n,
			status:     orderflow.StatusPending,
			payMethod:  "wechat",
			expireAt:   now.Add(-time.Minute),
		})
		_, _ = rig.queue.Enqueue(ctx, n, now.Add(-time.Minute))
	}

	w := NewCloseWorker(rig.engine, CloseOptions{PollBatchSize: 10, MaxWorkers: 4})
	_ = w.poll(ctx)

	// 等待派发的 goroutine 完成：wg.Wait 精确且幂等
	w.wg.Wait()

	// 交叉验证
	mustLen(t, rig.queue.acked, 3, "queue.acked")
	mustLen(t, rig.queue.reserved, 3, "queue.reserved")
	if len(rig.queue.pending) != 0 {
		t.Errorf("pending not empty: %+v", rig.queue.pending)
	}
	for _, n := range []string{"A", "B", "C"} {
		if rig.store.byNo[n].status != orderflow.StatusClosed {
			t.Errorf("order %s status = %v, want Closed", n, rig.store.byNo[n].status)
		}
	}
	mustEqual(t, rig.store.CloseCalls, 3, "Store.CASClose calls")
}

func TestCloseWorker_PollEmptyQueueNoOps(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()

	w := NewCloseWorker(rig.engine, CloseOptions{})
	_ = w.poll(ctx)
	w.wg.Wait()

	mustLen(t, rig.queue.reserved, 0, "reserved")
	mustLen(t, rig.queue.acked, 0, "acked")
	mustEqual(t, rig.store.CloseCalls, 0, "no CAS")
}

func TestCloseWorker_ProcessOneOnNotExpiredSkipsButAcks(t *testing.T) {
	// Engine.Close 对未过期订单直接 skip，不报错。
	// 这意味着消费者会 Ack（因为没 err），但订单状态保持 Pending。
	rig := newTestRig(t)
	ctx := context.Background()

	rig.store.seed(&testOrder{
		orderNo:    "FUTURE",
		orderToken: "tf",
		status:     orderflow.StatusPending,
		payMethod:  "wechat",
		expireAt:   time.Now().Add(time.Hour), // 未过期
	})
	_, _ = rig.queue.Enqueue(ctx, "FUTURE", time.Now().Add(-time.Second))

	w := NewCloseWorker(rig.engine, CloseOptions{PollBatchSize: 10, MaxWorkers: 2})
	_ = w.poll(ctx)
	w.wg.Wait()

	// 订单仍为 Pending
	mustEqual(t, rig.store.byNo["FUTURE"].status, orderflow.StatusPending, "status")
	// 任务被 Ack（Engine.Close 返回 nil 即视作已处理）
	mustLen(t, rig.queue.acked, 1, "acked")
}

func TestCloseWorker_RunStopsOnContextCancel(t *testing.T) {
	rig := newTestRig(t)
	w := NewCloseWorker(rig.engine, CloseOptions{PollInterval: 10 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// 正常退出
	case <-time.After(2 * time.Second):
		t.Fatal("CloseWorker.Run did not stop within timeout after cancel")
	}
}

// 断言辅助
func mustEqual[T comparable](t *testing.T, got, want T, msg string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", msg, got, want)
	}
}

func mustLen[T any](t *testing.T, got []T, want int, msg string) {
	t.Helper()
	if len(got) != want {
		t.Fatalf("%s: len=%d, want %d (got=%v)", msg, len(got), want, got)
	}
}
