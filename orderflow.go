package orderflow

import (
	"cmp"
	"fmt"
	"strings"
	"time"
)

// CloseSupersededPolicy 决定 Engine.Create 替代旧 Pending 单时，网关 CloseOrder 失败的处理策略。
//
// 场景：用户改了优惠券 / 支付方式，触发 closeSuperseded 关闭旧单，期间调用网关 CloseOrder。
// 网关返回 5xx 或网络超时（已带 3 次重试 + 渠道特定的可忽略错误识别）后仍失败时：
//
//   - SupersededStrict（默认）→ 直接返回错误，Create 失败。用户需重试整个下单流程。
//   - SupersededDegraded → 记 ALERT 日志，仍尝试本地 CAS Close 旧单，让 Create 继续。
//     旧网关订单的清理由 CloseFallback 周期扫描兜底（依赖 IsIgnorableCloseError 收敛）。
type CloseSupersededPolicy int8

const (
	// SupersededStrict 硬失败：网关关闭失败时直接返回错误，Create 失败。
	// 零值，向后兼容 v1.0.0 行为。适合"必须保证旧单已在网关侧关闭"的强一致性场景。
	SupersededStrict CloseSupersededPolicy = 0
	// SupersededDegraded 降级：网关关闭失败时记 ALERT 日志 + 走本地 CAS Close 让 Create 继续。
	// 推荐的生产配置——网关偶发抖动不应阻塞用户下新单。
	SupersededDegraded CloseSupersededPolicy = 1
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
	// Logger 日志接入。零值使用内置 nopLogger（丢弃所有日志）。
	//
	// 业务方需把自己的日志框架（推荐 github.com/gtkit/logger）包装成 Logger 接口
	// 注入。orderflow 核心包刻意不依赖任何具体日志框架——保持零外部依赖。
	// 包装示例与契约见 Logger 接口 GoDoc。
	Logger Logger

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

	// CloseSupersededPolicy 控制 Create 替代旧 Pending 单时，网关 CloseOrder 失败的策略。
	// 零值 SupersededStrict 保持 v1.0.0 行为；推荐生产环境改为 SupersededDegraded。
	// 详见 CloseSupersededPolicy 类型说明。
	CloseSupersededPolicy CloseSupersededPolicy
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
	orderExpire           time.Duration
	location              *time.Location
	logger                Logger
	observer              Observer
	locker                Locker // nil 表示未配置，Create 不加锁
	createLockTTL         time.Duration
	closeSupersededPolicy CloseSupersededPolicy
}

type dependencyValidator interface {
	Validate() error
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
	var logger Logger = cfg.Logger
	if logger == nil {
		logger = nopLogger{}
	}
	observer := cfg.Observer
	if observer == nil {
		observer = nopObserver{}
	}
	observer = wrapObserver(observer, logger)

	return &Engine[O]{
		store:                 cfg.Store,
		gateway:               cfg.Gateway,
		delayQueue:            cfg.DelayQueue,
		cache:                 cfg.Cache,
		stream:                cfg.Stream,
		onCreated:             cfg.OnCreated,
		onPaid:                cfg.OnPaid,
		onDelivered:           cfg.OnDelivered,
		onClosed:              cfg.OnClosed,
		onReopened:            cfg.OnReopened,
		onSuperseded:          cfg.OnSuperseded,
		onAnomaly:             cfg.OnAnomaly,
		isReusable:            cfg.IsReusable,
		resolveChannel:        cfg.ResolveChannel,
		buildNotifyURL:        cfg.BuildNotifyURL,
		generateOrderNo:       genOrderNo,
		generateOrderToken:    genOrderToken,
		orderExpire:           cmp.Or(cfg.OrderExpire, DefaultOrderExpire),
		location:              resolveLocation(cfg.Timezone),
		logger:                logger,
		observer:              observer,
		locker:                cfg.Locker, // 可为 nil
		createLockTTL:         cmp.Or(cfg.CreateLockTTL, DefaultCreateLockTTL),
		closeSupersededPolicy: cfg.CloseSupersededPolicy,
	}, nil
}

// DelayQueue 返回底层延时队列，供 worker 子包使用。
func (e *Engine[O]) DelayQueue() DelayQueue {
	return e.delayQueue
}

// Logger 返回 Engine 内部使用的 logger，便于 worker 子包复用同一配置。
func (e *Engine[O]) Logger() Logger {
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
	deps := []struct {
		name  string
		value any
	}{
		{"Store", c.Store},
		{"Gateway", c.Gateway},
		{"DelayQueue", c.DelayQueue},
		{"Cache", c.Cache},
		{"Stream", c.Stream},
	}

	var missing []string
	for _, dep := range deps {
		if dep.value == nil {
			missing = append(missing, dep.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %s", ErrMissingDep, strings.Join(missing, ", "))
	}

	for _, dep := range deps {
		validator, ok := dep.value.(dependencyValidator)
		if !ok {
			continue
		}
		if err := validator.Validate(); err != nil {
			return fmt.Errorf("%w: %s: %w", ErrInvalidConfig, dep.name, err)
		}
	}

	if c.Locker != nil {
		validator, ok := c.Locker.(dependencyValidator)
		if ok {
			if err := validator.Validate(); err != nil {
				return fmt.Errorf("%w: Locker: %w", ErrInvalidConfig, err)
			}
		}
	}

	if c.OrderExpire < 0 {
		return fmt.Errorf("%w: OrderExpire must be non-negative, got %s", ErrInvalidConfig, c.OrderExpire)
	}
	// 下界保护：低于 1s 几乎肯定是误填（订单还没下单就过期）。
	// **不设上界**：业务可能存在合法的长生命周期订单（押金、长租、预订等），由业务方
	// 自行确认 StatusCache 的 TTL 与之匹配。文档（CLAUDE.md / README）说明该约束。
	if c.OrderExpire > 0 && c.OrderExpire < time.Second {
		return fmt.Errorf("%w: OrderExpire %s is below the safe lower bound (1s)", ErrInvalidConfig, c.OrderExpire)
	}
	if c.CreateLockTTL < 0 {
		return fmt.Errorf("%w: CreateLockTTL must be non-negative, got %s", ErrInvalidConfig, c.CreateLockTTL)
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

