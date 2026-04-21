package worker

import "time"

const (
	defaultPollInterval  = time.Second
	defaultPollBatchSize = 50
	defaultPollLease     = 30 * time.Second
	defaultMaxWorkers    = 15
	defaultCloseTimeout  = 10 * time.Second
	defaultAckTimeout    = 3 * time.Second

	defaultCloseFallbackInterval  = 5 * time.Minute
	defaultCloseFallbackBatchSize = 200

	defaultDeliveryFallbackInterval  = time.Minute
	defaultDeliveryFallbackBatchSize = 100
)

// CloseOptions 控制 CloseWorker 的轮询节奏与并发。
// 零值字段会被替换为推荐默认值。
type CloseOptions struct {
	// PollInterval ReserveExpired 的轮询节拍。
	PollInterval time.Duration
	// PollBatchSize 每次尝试拉取的任务数量上限。
	PollBatchSize int
	// PollLease ReserveExpired 的租约时长；worker 崩溃后其他实例最快在租约过后可重入。
	PollLease time.Duration
	// MaxWorkers 同时处理关单的 goroutine 数。
	MaxWorkers int
	// CloseTimeout 单次 Engine.Close 调用的超时。
	CloseTimeout time.Duration
	// AckTimeout 单次队列 Ack 的超时。Ack 用独立 ctx，保证 worker 整体关停时也能把成功处理的任务确认掉。
	AckTimeout time.Duration
}

func (o CloseOptions) withDefaults() CloseOptions {
	if o.PollInterval <= 0 {
		o.PollInterval = defaultPollInterval
	}
	if o.PollBatchSize <= 0 {
		o.PollBatchSize = defaultPollBatchSize
	}
	if o.PollLease <= 0 {
		o.PollLease = defaultPollLease
	}
	if o.MaxWorkers <= 0 {
		o.MaxWorkers = defaultMaxWorkers
	}
	if o.CloseTimeout <= 0 {
		o.CloseTimeout = defaultCloseTimeout
	}
	if o.AckTimeout <= 0 {
		o.AckTimeout = defaultAckTimeout
	}
	return o
}

// CloseFallbackOptions 控制 CloseFallback 扫描节奏。
type CloseFallbackOptions struct {
	// Interval 扫描周期；生产建议 5min 起步，减轻 DB 压力。
	Interval time.Duration
	// BatchSize 每次扫描返回的订单号上限。
	BatchSize int
	// PerTaskTimeout 单个订单 Close 调用的超时。
	PerTaskTimeout time.Duration
}

func (o CloseFallbackOptions) withDefaults() CloseFallbackOptions {
	if o.Interval <= 0 {
		o.Interval = defaultCloseFallbackInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = defaultCloseFallbackBatchSize
	}
	if o.PerTaskTimeout <= 0 {
		o.PerTaskTimeout = defaultCloseTimeout
	}
	return o
}

// DeliveryFallbackOptions 控制 DeliveryFallback 扫描节奏。
type DeliveryFallbackOptions struct {
	// Interval 扫描周期；因 OnPaid 失败后延迟要尽量小，默认 1min。
	Interval time.Duration
	// BatchSize 每次扫描返回的订单号上限。
	BatchSize int
	// PerTaskTimeout 单个订单 ReconcilePaid 调用的超时。
	PerTaskTimeout time.Duration
}

func (o DeliveryFallbackOptions) withDefaults() DeliveryFallbackOptions {
	if o.Interval <= 0 {
		o.Interval = defaultDeliveryFallbackInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = defaultDeliveryFallbackBatchSize
	}
	if o.PerTaskTimeout <= 0 {
		o.PerTaskTimeout = defaultCloseTimeout
	}
	return o
}
