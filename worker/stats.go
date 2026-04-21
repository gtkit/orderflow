package worker

import (
	"sync/atomic"
	"time"
)

// Stats 是 worker 的运行时快照，用于监控接入（Prometheus gauge / healthcheck）。
//
// 字段语义：
//   - Inflight：当前正在处理的任务数（仅 CloseWorker 有意义，fallback 串行扫描 inflight 永远 ≤1）；
//   - LastPollAt：最近一次 poll/scan 开始时间。零值表示还未跑过。
//     如果 Now() - LastPollAt 远超 PollInterval，说明 worker 卡住了（go routine 阻塞在某 I/O）。
//   - LastPollDuration：最近一次 poll/scan 的耗时。
//   - LastBatchSize：最近一次 poll 拿到的任务数量。
//   - LastError：最近一次 poll/scan 遇到的错误（非 nil 表示 worker 正在退避状态）。
//   - PollsTotal / PollErrors：累计计数。
//
// 所有字段通过原子操作读写，调用方可从任意 goroutine 安全读取。
type Stats struct {
	Inflight         int64
	LastPollAt       time.Time
	LastPollDuration time.Duration
	LastBatchSize    int64
	LastError        string // error.Error()；空字符串表示上次成功
	PollsTotal       int64
	PollErrors       int64
}

// statsRecorder 是线程安全的运行时指标收集器。
type statsRecorder struct {
	inflight         atomic.Int64
	lastPollUnixNano atomic.Int64
	lastPollDuration atomic.Int64 // time.Duration
	lastBatchSize    atomic.Int64
	lastErr          atomic.Pointer[string] // nil 表示成功；*string 表示最近错误
	pollsTotal       atomic.Int64
	pollErrors       atomic.Int64
}

func (r *statsRecorder) snapshot() Stats {
	var lastErr string
	if ptr := r.lastErr.Load(); ptr != nil {
		lastErr = *ptr
	}
	var lastAt time.Time
	if ns := r.lastPollUnixNano.Load(); ns > 0 {
		lastAt = time.Unix(0, ns)
	}
	return Stats{
		Inflight:         r.inflight.Load(),
		LastPollAt:       lastAt,
		LastPollDuration: time.Duration(r.lastPollDuration.Load()),
		LastBatchSize:    r.lastBatchSize.Load(),
		LastError:        lastErr,
		PollsTotal:       r.pollsTotal.Load(),
		PollErrors:       r.pollErrors.Load(),
	}
}

func (r *statsRecorder) recordPollStart() time.Time {
	now := time.Now()
	r.lastPollUnixNano.Store(now.UnixNano())
	r.pollsTotal.Add(1)
	return now
}

func (r *statsRecorder) recordPollEnd(start time.Time, batchSize int, err error) {
	r.lastPollDuration.Store(int64(time.Since(start)))
	r.lastBatchSize.Store(int64(batchSize))
	if err != nil {
		r.pollErrors.Add(1)
		s := err.Error()
		r.lastErr.Store(&s)
	} else {
		r.lastErr.Store(nil)
	}
}
