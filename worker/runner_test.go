package worker

import (
	"context"
	"testing"
	"time"
)

func TestStartAll_SpinsUpThreeWorkersAndStops(t *testing.T) {
	rig := newTestRig(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		// 用 Options 把 interval 调小，不然默认 1s/1min/5min 太慢
		StartAllWithOptions(ctx, rig.engine, Options{
			Close:            CloseOptions{PollInterval: 10 * time.Millisecond},
			CloseFallback:    CloseFallbackOptions{Interval: 20 * time.Millisecond},
			DeliveryFallback: DeliveryFallbackOptions{Interval: 20 * time.Millisecond},
		})
		close(done)
	}()

	// 让三个 worker 跑一会儿
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("StartAll did not exit after ctx cancel")
	}
}

func TestOptions_WithDefaults(t *testing.T) {
	c := CloseOptions{}.withDefaults()
	if c.PollInterval <= 0 || c.PollBatchSize <= 0 || c.PollLease <= 0 ||
		c.MaxWorkers <= 0 || c.CloseTimeout <= 0 || c.AckTimeout <= 0 {
		t.Errorf("CloseOptions defaults incomplete: %+v", c)
	}

	cf := CloseFallbackOptions{}.withDefaults()
	if cf.Interval <= 0 || cf.BatchSize <= 0 || cf.PerTaskTimeout <= 0 {
		t.Errorf("CloseFallbackOptions defaults incomplete: %+v", cf)
	}

	df := DeliveryFallbackOptions{}.withDefaults()
	if df.Interval <= 0 || df.BatchSize <= 0 || df.PerTaskTimeout <= 0 {
		t.Errorf("DeliveryFallbackOptions defaults incomplete: %+v", df)
	}

	// 显式非零值不应被覆盖
	c2 := CloseOptions{PollBatchSize: 99}.withDefaults()
	if c2.PollBatchSize != 99 {
		t.Errorf("override PollBatchSize was reset: %+v", c2)
	}
}
