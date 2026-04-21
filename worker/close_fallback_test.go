package worker

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderflow"
)

// CloseFallback 周期扫描已过期 Pending 并调 Close，兜底 CloseWorker 漏掉的任务。

func TestCloseFallback_ScanClosesAllExpiredPending(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	now := time.Now()

	// 过期未关单（CloseWorker 漏掉的场景：Redis 掉数据）
	for _, n := range []string{"X", "Y"} {
		rig.store.seed(&testOrder{
			orderNo:    n,
			orderToken: "t-" + n,
			status:     orderflow.StatusPending,
			payMethod:  "wechat",
			expireAt:   now.Add(-time.Hour),
		})
		// 注意：不入延时队列，模拟 Redis 数据丢失
	}
	// 未过期的不应被扫
	rig.store.seed(&testOrder{
		orderNo:    "FUTURE",
		orderToken: "tf",
		status:     orderflow.StatusPending,
		payMethod:  "wechat",
		expireAt:   now.Add(time.Hour),
	})

	f := NewCloseFallback(rig.engine, CloseFallbackOptions{BatchSize: 10})
	f.scan(ctx)

	mustEqual(t, rig.store.byNo["X"].status, orderflow.StatusClosed, "X closed")
	mustEqual(t, rig.store.byNo["Y"].status, orderflow.StatusClosed, "Y closed")
	mustEqual(t, rig.store.byNo["FUTURE"].status, orderflow.StatusPending, "FUTURE unchanged")
}

func TestCloseFallback_EmptyScanNoOps(t *testing.T) {
	rig := newTestRig(t)
	f := NewCloseFallback(rig.engine, CloseFallbackOptions{})
	f.scan(context.Background())
	mustEqual(t, rig.store.CloseCalls, 0, "no CAS on empty")
}

func TestCloseFallback_RunStopsOnContextCancel(t *testing.T) {
	rig := newTestRig(t)
	f := NewCloseFallback(rig.engine, CloseFallbackOptions{Interval: 10 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.Run(ctx)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("CloseFallback.Run did not stop")
	}
}
