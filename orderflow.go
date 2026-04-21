package orderflow

import (
	"cmp"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"
)

// Config 是 Engine 的全部配置项。
//
//   - 能力接口（Store / Gateway / DelayQueue / Cache / Stream）必填，缺失会返回 ErrMissingDep；
//   - 业务钩子全部可选；未设置则使用内置默认或视为无操作；
//   - 参数字段（OrderExpire / Timezone / Logger）有合理默认值。
type Config[O OrderSnapshot] struct {
	// ----- 能力接口（必填） -----

	Store      Store[O]
	Gateway    PaymentGateway
	DelayQueue DelayQueue
	Cache      StatusCache
	Stream     StatusStream

	// ----- 业务钩子（可选） -----

	OnCreated    OnCreatedHook[O]
	OnPaid       OnPaidHook[O]
	OnDelivered  OnDeliveredHook[O]
	OnClosed     OnClosedHook[O]
	OnReopened   OnReopenedHook[O]
	OnSuperseded OnSupersededHook[O]
	OnAnomaly    OnAnomalyHook[O]

	IsReusable         IsReusableFunc[O]
	ResolveChannel     ResolveChannelFunc
	BuildNotifyURL     BuildNotifyURLFunc
	GenerateOrderNo    GenerateOrderNoFunc
	GenerateOrderToken GenerateOrderTokenFunc

	// ----- 参数 -----

	// OrderExpire 订单支付有效期。零值使用 DefaultOrderExpire。
	OrderExpire time.Duration
	// Timezone IANA 时区名（如 "Asia/Shanghai"）。空或解析失败时使用 time.Local。
	Timezone string
	// Logger 日志实例。零值使用将日志丢弃的 nop logger（避免无意义噪音）。
	// 接入 gtkit/logger：slog.New(logger.SlogHandler())。
	Logger *slog.Logger

	// Observer metrics / tracing 观测器。零值使用 nopObserver（零开销）。
	// 业务侧注入 Prometheus / OpenTelemetry adapter 时必须保证非阻塞 + 不 panic。
	Observer Observer

	// Locker 分布式锁（可选）。配置后 Engine.Create 会在 (user_id, product_id) 维度
	// 串行化，避免并发下单产生多个 Pending。零值不加锁（行为同前）。
	//
	// 推荐配合 DB 部分唯一索引做"前端 debounce + 应用层锁 + DB 兜底"三层防御。
	Locker Locker

	// CreateLockTTL Locker 持锁时长上限。短于 Create 实际耗时（含 UnifiedOrder 网络 RTT）
	// 会导致锁提前释放。零值使用 DefaultCreateLockTTL（10s）。
	CreateLockTTL time.Duration
}

// Engine 是订单流程的核心编排器。
//
// 泛型参数 O 是业务方的订单类型，必须实现 OrderSnapshot。
// Engine 构造后即不可变，方法集对并发调用安全（安全性继承自底层 Store / Cache 等）。
type Engine[O OrderSnapshot] struct {
	// 能力接口
	store      Store[O]
	gateway    PaymentGateway
	delayQueue DelayQueue
	cache      StatusCache
	stream     StatusStream

	// 钩子
	onCreated    OnCreatedHook[O]
	onPaid       OnPaidHook[O]
	onDelivered  OnDeliveredHook[O]
	onClosed     OnClosedHook[O]
	onReopened   OnReopenedHook[O]
	onSuperseded OnSupersededHook[O]
	onAnomaly    OnAnomalyHook[O]

	isReusable         IsReusableFunc[O]
	resolveChannel     ResolveChannelFunc
	buildNotifyURL     BuildNotifyURLFunc
	generateOrderNo    GenerateOrderNoFunc
	generateOrderToken GenerateOrderTokenFunc

	// 参数
	orderExpire   time.Duration
	location      *time.Location
	logger        *slog.Logger
	observer      Observer
	locker        Locker // nil 表示未配置，Create 不加锁
	createLockTTL time.Duration
}

// New 构造 Engine。能力接口为 nil 时返回 ErrMissingDep；参数非法时返回 ErrInvalidConfig。
func New[O OrderSnapshot](cfg Config[O]) (*Engine[O], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	genOrderNo := cfg.GenerateOrderNo
	if genOrderNo == nil {
		genOrderNo = defaultGenerateOrderNo
	}
	genOrderToken := cfg.GenerateOrderToken
	if genOrderToken == nil {
		genOrderToken = defaultGenerateOrderToken
	}
	logger := cfg.Logger
	if logger == nil {
		logger = nopLogger()
	}
	observer := cfg.Observer
	if observer == nil {
		observer = nopObserver{}
	}

	return &Engine[O]{
		store:              cfg.Store,
		gateway:            cfg.Gateway,
		delayQueue:         cfg.DelayQueue,
		cache:              cfg.Cache,
		stream:             cfg.Stream,
		onCreated:          cfg.OnCreated,
		onPaid:             cfg.OnPaid,
		onDelivered:        cfg.OnDelivered,
		onClosed:           cfg.OnClosed,
		onReopened:         cfg.OnReopened,
		onSuperseded:       cfg.OnSuperseded,
		onAnomaly:          cfg.OnAnomaly,
		isReusable:         cfg.IsReusable,
		resolveChannel:     cfg.ResolveChannel,
		buildNotifyURL:     cfg.BuildNotifyURL,
		generateOrderNo:    genOrderNo,
		generateOrderToken: genOrderToken,
		orderExpire:        cmp.Or(cfg.OrderExpire, DefaultOrderExpire),
		location:           resolveLocation(cfg.Timezone),
		logger:             logger,
		observer:           observer,
		locker:             cfg.Locker, // 可为 nil
		createLockTTL:      cmp.Or(cfg.CreateLockTTL, DefaultCreateLockTTL),
	}, nil
}

// DelayQueue 返回底层延时队列，供 worker 子包使用。
func (e *Engine[O]) DelayQueue() DelayQueue {
	return e.delayQueue
}

// Logger 返回 Engine 内部使用的 logger，便于 worker 子包复用同一配置。
func (e *Engine[O]) Logger() *slog.Logger {
	return e.logger
}

// OrderExpire 返回订单支付有效期。
func (e *Engine[O]) OrderExpire() time.Duration {
	return e.orderExpire
}

// Location 返回 Engine 使用的时区。
func (e *Engine[O]) Location() *time.Location {
	return e.location
}

// validate 校验 Config 的必填项与合法性。
func (c Config[O]) validate() error {
	var missing []string
	if c.Store == nil {
		missing = append(missing, "Store")
	}
	if c.Gateway == nil {
		missing = append(missing, "Gateway")
	}
	if c.DelayQueue == nil {
		missing = append(missing, "DelayQueue")
	}
	if c.Cache == nil {
		missing = append(missing, "Cache")
	}
	if c.Stream == nil {
		missing = append(missing, "Stream")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %s", ErrMissingDep, strings.Join(missing, ", "))
	}

	if c.OrderExpire < 0 {
		return fmt.Errorf("%w: OrderExpire must be non-negative, got %s", ErrInvalidConfig, c.OrderExpire)
	}
	return nil
}

// resolveLocation 解析 IANA 时区名，非法或空值时退回 time.Local。
func resolveLocation(tz string) *time.Location {
	if strings.TrimSpace(tz) == "" {
		return time.Local
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.Local
	}
	return loc
}

// nopLogger 返回一个丢弃全部输出的 slog.Logger，用于未注入 Logger 时的安全退路。
func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
