package orderflow

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// 探针实现：fakeStore / fakeCache 等不实现 Healther，Engine.Healthy 跳过它们后返回 nil。
func TestHealthy_NoHealtherDepsReturnsNil(t *testing.T) {
	env := newTestEnv(t)
	if err := env.engine.Healthy(context.Background()); err != nil {
		t.Fatalf("Healthy without Healther deps should be nil, got %v", err)
	}
}

// 注入一个会失败的 Healther，验证错误聚合。
// 用 fakeCacheWithPing 把 Engine.cache 替换成实现 Healther 的实例。
type fakeCacheWithPing struct {
	*fakeCache
	pingErr error
}

func (c *fakeCacheWithPing) Ping(_ context.Context) error { return c.pingErr }

func TestHealthy_AggregatesErrors(t *testing.T) {
	env := newTestEnv(t)
	env.engine.cache = &fakeCacheWithPing{
		fakeCache: env.cache,
		pingErr:   errors.New("redis down"),
	}
	err := env.engine.Healthy(context.Background())
	if err == nil {
		t.Fatal("expected error from Healthy when cache Ping fails")
	}
	if !strings.Contains(err.Error(), "Cache") || !strings.Contains(err.Error(), "redis down") {
		t.Errorf("error should reference Cache + reason, got %v", err)
	}
}

// P2#6: PaymentGateway 实现 Healther 时 Engine.Healthy 必须包含它。
type fakeGatewayWithPing struct {
	*fakeGateway
	pingErr error
}

func (g *fakeGatewayWithPing) Ping(_ context.Context) error { return g.pingErr }

func TestHealthy_ProbesPaymentGatewayWhenImplemented(t *testing.T) {
	t.Run("Gateway 实现 Healther 且失败 -> Healthy 返回带 PaymentGateway 前缀的错误", func(t *testing.T) {
		env := newTestEnv(t)
		env.engine.gateway = &fakeGatewayWithPing{
			fakeGateway: env.gw,
			pingErr:     errors.New("alipay api unreachable"),
		}
		err := env.engine.Healthy(context.Background())
		if err == nil {
			t.Fatal("expected error from Healthy when gateway Ping fails")
		}
		if !strings.Contains(err.Error(), "PaymentGateway") || !strings.Contains(err.Error(), "alipay api unreachable") {
			t.Errorf("error should reference PaymentGateway + reason, got %v", err)
		}
	})

	t.Run("Gateway 不实现 Healther -> Healthy 不报错（保留历史行为）", func(t *testing.T) {
		env := newTestEnv(t)
		// env.gw 本身不实现 Healther
		if err := env.engine.Healthy(context.Background()); err != nil {
			t.Fatalf("expected nil error when gateway lacks Healther, got %v", err)
		}
	})
}
