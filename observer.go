package orderflow

import (
	"context"
	"time"
)

// Observer 观测 Engine 运行时事件，用于埋 metrics / tracing / 结构化日志。
//
// 设计分离：
//   - Event：记录**状态跃迁**或**异常**（Created / Paid / Delivered / Closed / Reopened / Anomaly）。
//     Prometheus 侧可翻译为 counter，OpenTelemetry 侧可作为 span event。
//   - Duration：记录**操作耗时**（含成功 / 失败标志）。Prometheus 侧对应 histogram，
//     OTel 侧对应 span duration。
//
// 实现契约（必须遵守）：
//   - 所有方法必须非阻塞。Observer 调用位于 Engine 的热路径上，
//     实现方做的 I/O（上报到 StatsD / Prometheus pushgateway / 发日志等）必须异步 +
//     带缓冲 + 丢弃溢出样本。
//   - 禁止 panic。Engine 不对 Observer 的实现加 recover；panic 会直接冲破调用链。
//   - 读取 ctx 中的 trace span 等值是允许的，但不得修改 ctx 生命周期。
//
// 默认实现（未注入时）是 nopObserver，零开销无分配。
type Observer interface {
	// Event 记录状态跃迁或异常事件。
	//   ctx:      当前调用 ctx，可能携带 trace span。
	//   kind:     事件类别，见 EventKind* 常量。
	//   orderNo:  订单号，可能为空（如 Create 早期失败）。
	//   attrs:    额外标签（trade_no / anomaly kind / reason 等）。实现方用完即释放，
	//             禁止长时间持有 map；Engine 不保证 map 在调用返回后仍有效。
	Event(ctx context.Context, kind EventKind, orderNo string, attrs map[string]any)

	// Duration 记录一次操作耗时。
	//   op:       操作名，"Create" / "HandleNotify" / "Close" / "ReconcilePaid"。
	//   d:        操作耗时。
	//   err:      操作最终错误；nil 表示成功。
	Duration(ctx context.Context, op string, d time.Duration, err error)
}

// EventKind 是 Observer.Event 的分类枚举。
// 业务侧用这些值做 Prometheus label 或 OTel span attribute。
type EventKind string

const (
	// EventOrderCreated 新订单创建并进入 Pending。
	EventOrderCreated EventKind = "order_created"
	// EventOrderReused 命中已有 Pending 订单，复用（未新建）。
	EventOrderReused EventKind = "order_reused"
	// EventOrderSuperseded 旧订单被同用户新订单取代而关闭。
	EventOrderSuperseded EventKind = "order_superseded"
	// EventOrderPaid CAS 推进到 Paid 状态（支付回调到达并通过校验）。
	EventOrderPaid EventKind = "order_paid"
	// EventOrderDelivered 履约完成，订单进入 Delivered。
	EventOrderDelivered EventKind = "order_delivered"
	// EventOrderClosed 订单因超时 / 主动关闭 / 取代等进入 Closed。
	EventOrderClosed EventKind = "order_closed"
	// EventOrderReopened "Closed 后又被网关确认已支付"的恢复路径。
	EventOrderReopened EventKind = "order_reopened"
	// EventAnomaly 订单异常（金额不符 / 交易号不符 / 意外状态等）。
	EventAnomaly EventKind = "anomaly"
)

// Operation 名常量，供 Duration 方法使用。
const (
	OpCreate        = "Create"
	OpHandleNotify  = "HandleNotify"
	OpClose         = "Close"
	OpReconcilePaid = "ReconcilePaid"
	OpPollStatus    = "PollStatus"
)

// nopObserver 是 Observer 的零开销默认实现。
type nopObserver struct{}

func (nopObserver) Event(context.Context, EventKind, string, map[string]any) {}
func (nopObserver) Duration(context.Context, string, time.Duration, error)   {}

// safeObserver 为注入的 Observer 实现加 panic recover 防御。
//
// 设计背景：Observer 约定 "禁止 panic"（见 Observer 接口注释），但约定靠自觉，
// 业务侧接 Prometheus / OpenTelemetry 时一旦某个 adapter 出 bug，panic 会冲破 Engine
// 主流程——订单可能已 CAS 到 Paid 但整个 HandleNotify 异常退出。safeObserver 把这种
// 第三方实现错误隔离到 Observer 自身，Engine 内部始终视 Observer 为 "绝不失败"。
//
// 对 nopObserver 不包装（零开销路径保持原样）。
type safeObserver struct {
	inner  Observer
	logger Logger
}

func wrapObserver(inner Observer, logger Logger) Observer {
	if _, ok := inner.(nopObserver); ok {
		return inner
	}
	if _, ok := inner.(*safeObserver); ok {
		return inner
	}
	return &safeObserver{inner: inner, logger: logger}
}

func (s *safeObserver) Event(ctx context.Context, kind EventKind, orderNo string, attrs map[string]any) {
	defer func() {
		if r := recover(); r != nil && s.logger != nil {
			s.logger.Error(ctx, "orderflow: observer.Event panic recovered",
				String("event_kind", string(kind)),
				String("order_no", orderNo),
				Any("panic", r),
			)
		}
	}()
	s.inner.Event(ctx, kind, orderNo, attrs)
}

func (s *safeObserver) Duration(ctx context.Context, op string, d time.Duration, err error) {
	defer func() {
		if r := recover(); r != nil && s.logger != nil {
			s.logger.Error(ctx, "orderflow: observer.Duration panic recovered",
				String("op", op),
				Any("panic", r),
			)
		}
	}()
	s.inner.Duration(ctx, op, d, err)
}
