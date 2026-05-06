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
