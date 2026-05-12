package orderflow

import (
	"context"
	"errors"
	"fmt"
)

// Healther 是依赖能力的可选接口：实现后可被 Engine.Healthy 探测。
//
// driver 自己决定 Ping 的语义——例如 GORM 走 `db.PingContext`，go-redis 走 `Ping(ctx)`。
// 对成本敏感的依赖（如 PaymentGateway 调用第三方 API）可以选择不实现 Healther，
// Engine.Healthy 会跳过它。
type Healther interface {
	// Ping 验证依赖在 ctx 超时窗口内可用。失败返回非 nil error。
	Ping(ctx context.Context) error
}

// Healthy 探测所有可达的能力依赖，用于 K8s readiness probe / 启动自检。
//
// 检查路径：Store / Cache / Stream / DelayQueue / Locker / **PaymentGateway**
// （如配置且实现 Healther）逐个 Ping，任一失败即返回错误（带依赖名称前缀）。
//
// **关于 PaymentGateway**：driver 可选实现 Healther——网关的"健康"语义多变
// （白名单 IP、签名密钥、商户号），driver 应自行选择轻量探测策略（如做一个无副作用的
// QueryOrder("__healthprobe__") 并把 ErrOrderNotFound 视作健康）。drivers/paymgrgw
// 未实现 Healther 时本探测自动跳过——保留与历史行为兼容（Gateway 不可达不影响 readiness）。
//
// **关于 RefundGateway**：核心 Engine 不持有 RefundGateway 引用（退款流程由业务方
// 自行编排，详见 refund_gateway.go），因此 Healthy 不主动探测。业务方在退款服务的
// readiness 探测里应单独验证 RefundGateway——如果 driver 同时实现 PaymentGateway 与
// RefundGateway（典型如 *paymgrgw.Gateway），探测 PaymentGateway 等于隐式覆盖 RefundGateway。
//
// 不实现 Healther 接口的依赖会被跳过（视为"健康"）；建议生产 driver 都实现。
//
// 调用方应给出独立 ctx + timeout，避免占用应用主流程。典型用法：
//
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
//	defer cancel()
//	if err := engine.Healthy(ctx); err != nil {
//	    w.WriteHeader(503)
//	    return
//	}
func (e *Engine[O]) Healthy(ctx context.Context) error {
	deps := []struct {
		name string
		dep  any
	}{
		{"Store", e.store},
		{"Cache", e.cache},
		{"Stream", e.stream},
		{"DelayQueue", e.delayQueue},
		{"Locker", e.locker},
		{"PaymentGateway", e.gateway},
	}
	var errs []error
	for _, d := range deps {
		if d.dep == nil {
			continue
		}
		h, ok := d.dep.(Healther)
		if !ok {
			continue
		}
		if err := h.Ping(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", d.name, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("orderflow: unhealthy dependencies: %w", errors.Join(errs...))
	}
	return nil
}
