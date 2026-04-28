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
// 检查路径：Store / Cache / Stream / DelayQueue / Locker（如配置）逐个 Ping，
// 任一失败即返回错误（带依赖名称前缀）。Gateway 默认不探测——网关的"健康"语义
// 多变（白名单 IP、签名密钥、商户号），由业务方在网关 driver 内自行实现。
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
