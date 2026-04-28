package orderflow

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"
)

// safeHook 调用一个无返回值的业务钩子，并捕获 panic。
//
// Engine 不对业务钩子的实现加 recover 是历史决定（钩子约定"不应 panic"），但约定靠
// 自觉——业务侧接 Prometheus / OpenTelemetry / 第三方 SDK 时一旦 panic，会冲破
// HandleNotify / Create 主流程：CAS 已成功但请求异常退出，支付网关收不到 ACK 触发
// 重试风暴。这里把第三方实现错误隔离到钩子自身，让 Engine 始终视钩子为"绝不失败"。
//
// orderNo 可能为空（比如钩子绑在尚未生成订单号的早期路径上）。
func (e *Engine[O]) safeHook(ctx context.Context, name, orderNo string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error(ctx, "orderflow: hook panic recovered",
				String("hook", name),
				String("order_no", orderNo),
				Any("panic", r),
			)
			e.observer.Event(ctx, EventAnomaly, orderNo, map[string]any{
				"kind":  "hook_panic",
				"hook":  name,
				"panic": fmt.Sprint(r),
			})
		}
	}()
	fn()
}

// safeHookE 调用一个返回 error 的业务钩子，并把 panic 转成 error。
//
// 与 safeHook 不同的是：error-returning 钩子（OnPaid / OnCreated / OnDelivered）的
// 错误会被主流程消费——OnPaid 失败会触发补偿重试。把 panic 转成 error 让补偿路径
// 仍然有机会兜底，而不是中断整个 HandleNotify。
func (e *Engine[O]) safeHookE(ctx context.Context, name, orderNo string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("orderflow: hook %s panic: %v", name, r)
			e.logger.Error(ctx, "orderflow: hook panic recovered",
				String("hook", name),
				String("order_no", orderNo),
				Any("panic", r),
			)
			e.observer.Event(ctx, EventAnomaly, orderNo, map[string]any{
				"kind":  "hook_panic",
				"hook":  name,
				"panic": fmt.Sprint(r),
			})
		}
	}()
	return fn()
}

// publishStatus 将订单状态同步到缓存与订阅通道。
// 缓存失败视为严重错误（会回删保证一致性），推送失败仅降级为轮询（警告）。
//
// 双重失败路径（Set 失败 + Delete 也失败）会升级为 ERROR 日志——此时缓存里可能残留
// 更早的状态，APP 轮询会读到错误值。运维应基于 "orderflow: ALERT cache inconsistent"
// 关键字配置告警。
func (e *Engine[O]) publishStatus(ctx context.Context, orderToken string, userID int64, status OrderStatus, expireAt time.Time) {
	if err := e.cache.Set(ctx, orderToken, userID, status, expireAt); err != nil {
		e.logger.Warn(ctx, "orderflow: set status cache failed, delete for consistency",
			String("order_token", orderToken),
			String("status", status.String()),
			Err(err),
		)
		e.observer.Event(ctx, EventAnomaly, "", map[string]any{
			"kind":        "publish_status_cache_set_failed",
			"order_token": orderToken,
			"status":      status.String(),
			"reason":      err.Error(),
		})
		if delErr := e.cache.Delete(ctx, orderToken); delErr != nil {
			e.logger.Error(ctx, "orderflow: ALERT cache inconsistent: set and delete both failed",
				String("order_token", orderToken),
				String("status", status.String()),
				Any("set_error", err),
				Any("delete_error", delErr),
			)
			e.observer.Event(ctx, EventAnomaly, "", map[string]any{
				"kind":         "publish_status_cache_inconsistent",
				"order_token":  orderToken,
				"status":       status.String(),
				"set_error":    err.Error(),
				"delete_error": delErr.Error(),
			})
		}
		return
	}
	if err := e.stream.Publish(ctx, orderToken, status); err != nil {
		e.logger.Warn(ctx, "orderflow: publish status failed, clients will fallback to polling",
			String("order_token", orderToken),
			String("status", status.String()),
			Err(err),
		)
		e.observer.Event(ctx, EventAnomaly, "", map[string]any{
			"kind":        "publish_status_stream_failed",
			"order_token": orderToken,
			"status":      status.String(),
			"reason":      err.Error(),
		})
	}
}

// appendLog 写一条订单状态流水。失败降级为警告日志 + Observer 异常事件，不阻断主流程。
//
// 发 EventAnomaly 的目的：审计/合规场景下流水丢失是严重事件——DBA 误删 log 表、
// 表满、磁盘满等情况下，仅记本地 warn 日志运维不会感知。Observer 事件让 Prometheus
// 侧能基于 anomaly counter 配置告警。
func (e *Engine[O]) appendLog(ctx context.Context, order O, from, to OrderStatus, actor, remark string) {
	entry := LogEntry{
		OrderNo:    order.OrderNo(),
		UserID:     order.UserID(),
		FromStatus: from,
		ToStatus:   to,
		Actor:      actor,
		Remark:     remark,
		CreatedAt:  time.Now(),
	}
	if err := e.store.AppendLog(ctx, entry); err != nil {
		e.logger.Warn(ctx, "orderflow: append order log failed",
			String("order_no", order.OrderNo()),
			String("from", from.String()),
			String("to", to.String()),
			Err(err),
		)
		e.observer.Event(ctx, EventAnomaly, order.OrderNo(), map[string]any{
			"kind":   "append_log_failed",
			"from":   from.String(),
			"to":     to.String(),
			"actor":  actor,
			"reason": err.Error(),
		})
	}
}

// recordAnomaly 记录业务异常：打 error 日志 + 触发 Observer 事件 + 调用 OnAnomaly 钩子（如配置）。
func (e *Engine[O]) recordAnomaly(ctx context.Context, order O, kind AnomalyKind, detail string) {
	e.logger.Error(ctx, "orderflow: ALERT anomaly",
		String("order_no", order.OrderNo()),
		String("kind", string(kind)),
		String("detail", detail),
	)
	e.observer.Event(ctx, EventAnomaly, order.OrderNo(), map[string]any{
		"kind":   string(kind),
		"detail": detail,
	})
	if e.onAnomaly != nil {
		e.safeHook(ctx, "OnAnomaly", order.OrderNo(), func() {
			e.onAnomaly(ctx, order, kind, detail)
		})
	}
}

// resolveChannelOf 将业务语义的支付方式映射为网关渠道。
// 未配置钩子时直接将 payMethod 当作 Channel（适用于 "wechat" / "alipay" 这类一致命名）。
func (e *Engine[O]) resolveChannelOf(payMethod string) Channel {
	if e.resolveChannel != nil {
		return e.resolveChannel(payMethod)
	}
	return Channel(payMethod)
}

// buildNotifyURLOf 构造支付回调 URL。
// 未配置钩子时返回相对路径 "/<url-escaped channel>"——生产环境建议显式注入钩子以包含完整域名。
//
// **安全约束**：默认实现用 url.PathEscape 保护 channel 值，避免未经业务方校验的
// PayMethod 通过 resolveChannelOf 传进来后产生 "/../admin" 之类的路径遍历。
// 业务方应在 Config.ResolveChannel 钩子里做白名单校验，而不是依赖此处的 escape。
func (e *Engine[O]) buildNotifyURLOf(ch Channel) string {
	if e.buildNotifyURL != nil {
		return e.buildNotifyURL(ch)
	}
	return "/" + url.PathEscape(string(ch))
}

// isReusableOf 判断已有 Pending 订单是否可以被复用。
// 默认：原价一致且支付方式一致则可复用（与 sleep_client 现网行为一致）。
func (e *Engine[O]) isReusableOf(existing O, req CreateRequest) bool {
	if e.isReusable != nil {
		return e.isReusable(existing, req)
	}
	return existing.OriginalPrice() == req.Product.Price && existing.PayMethod() == req.PayMethod
}

// retryN 以固定次数尝试 fn，每次失败 sleep delay。
//
// ctx 感知细节：
//   - 每次尝试前先查 ctx.Err()，避免 ctx 已取消还白跑一次 fn；
//   - fn 返回的错误若本身已是 context.Canceled / context.DeadlineExceeded，
//     说明底层调用已经感知取消，不重试直接返回（避免浪费额外 attempt × delay）；
//   - sleep 阶段监听 ctx.Done，取消时立即返回 ctx.Err()。
func retryN[T any](ctx context.Context, attempts int, delay time.Duration, fn func() (T, error)) (T, error) {
	var (
		result T
		err    error
	)
	for i := range attempts {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, ctxErr
		}
		result, err = fn()
		if err == nil {
			return result, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return result, err
		}
		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return result, err
}
