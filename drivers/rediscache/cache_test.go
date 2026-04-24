package rediscache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gtkit/orderflow"
	"github.com/redis/go-redis/v9"
)

// =============================================================================
// Test infrastructure
// =============================================================================

func newTestCache(t *testing.T, opts ...CacheOption) (*StatusCache, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewStatusCache(rdb, opts...), server, rdb
}

// =============================================================================
// StatusCache Set/Get/Delete
// =============================================================================

func TestCache_SetGetRoundtrip(t *testing.T) {
	c, _, _ := newTestCache(t)
	ctx := context.Background()

	err := c.Set(ctx, "TOK-1", 1001, orderflow.StatusPaid, time.Time{})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, hit, err := c.Get(ctx, "TOK-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !hit {
		t.Fatal("expected hit")
	}
	if got.Status != orderflow.StatusPaid {
		t.Errorf("Status = %v, want Paid", got.Status)
	}
	if got.UserID != 1001 {
		t.Errorf("UserID = %d, want 1001", got.UserID)
	}
}

func TestCache_MissReturnsFalse(t *testing.T) {
	c, _, _ := newTestCache(t)
	got, hit, err := c.Get(context.Background(), "UNKNOWN")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if hit {
		t.Fatal("expected miss, got hit")
	}
	if got.UserID != 0 || got.Status != 0 {
		t.Errorf("expected zero CachedStatus, got %+v", got)
	}
}

func TestCache_Delete(t *testing.T) {
	c, _, _ := newTestCache(t)
	ctx := context.Background()
	_ = c.Set(ctx, "TOK-D", 1001, orderflow.StatusPaid, time.Time{})

	if err := c.Delete(ctx, "TOK-D"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, hit, _ := c.Get(ctx, "TOK-D")
	if hit {
		t.Fatal("expected miss after delete")
	}
}

// Self-heal：旧格式 / 脏数据按 miss 处理，避免卡死用户
func TestCache_MalformedValueSelfHeals(t *testing.T) {
	c, server, _ := newTestCache(t)
	ctx := context.Background()
	// 直接往 Redis 写一个非法的 value
	_ = server.Set("orderflow:status:DIRTY", "not-parseable")

	_, hit, err := c.Get(ctx, "DIRTY")
	if err != nil {
		t.Fatalf("Get on malformed should not error, got: %v", err)
	}
	if hit {
		t.Fatal("expected miss on malformed value")
	}
}

// =============================================================================
// TTL 派发
// =============================================================================

func TestCache_TTLForActiveStatus(t *testing.T) {
	c, server, _ := newTestCache(t)
	ctx := context.Background()

	// Paid / Delivered / Completed 默认 5min
	for _, s := range []orderflow.OrderStatus{orderflow.StatusPaid, orderflow.StatusDelivered, orderflow.StatusCompleted} {
		token := "TOK-" + s.String()
		_ = c.Set(ctx, token, 1001, s, time.Time{})
		ttl := server.TTL(c.key(token))
		if ttl != 5*time.Minute {
			t.Errorf("%s TTL = %s, want 5m", s, ttl)
		}
	}
}

func TestCache_TTLForTerminalStatus(t *testing.T) {
	c, server, _ := newTestCache(t)
	ctx := context.Background()

	// Closed / Cancelled 默认 2min
	for _, s := range []orderflow.OrderStatus{orderflow.StatusClosed, orderflow.StatusCancelled} {
		token := "TOK-" + s.String()
		_ = c.Set(ctx, token, 1001, s, time.Time{})
		ttl := server.TTL(c.key(token))
		if ttl != 2*time.Minute {
			t.Errorf("%s TTL = %s, want 2m", s, ttl)
		}
	}
}

func TestCache_TTLForPendingUsesExpireAtPlusGrace(t *testing.T) {
	c, server, _ := newTestCache(t)
	ctx := context.Background()

	expireAt := time.Now().Add(30 * time.Minute)
	_ = c.Set(ctx, "TOK-P", 1001, orderflow.StatusPending, expireAt)

	// 默认 PendingGrace=5min；总 TTL 约 35min（miniredis 精度到秒）
	ttl := server.TTL(c.key("TOK-P"))
	if ttl < 34*time.Minute || ttl > 36*time.Minute {
		t.Errorf("Pending TTL = %s, want ~35m", ttl)
	}
}

func TestCache_TTLForPendingAlreadyExpiredFallsBack(t *testing.T) {
	c, server, _ := newTestCache(t)
	ctx := context.Background()

	// Pending 的 expireAt 已经过去，应该退回 FallbackTTL（默认 2min）
	_ = c.Set(ctx, "TOK-EXP", 1001, orderflow.StatusPending, time.Now().Add(-time.Hour))
	ttl := server.TTL(c.key("TOK-EXP"))
	if ttl != 2*time.Minute {
		t.Errorf("expired-Pending TTL = %s, want 2m (fallback)", ttl)
	}
}

func TestCache_TTLUnknownStatusFallsBack(t *testing.T) {
	c, server, _ := newTestCache(t)
	ctx := context.Background()

	// StatusUnknown 不在默认 TTL map，应该用 FallbackTTL
	_ = c.Set(ctx, "TOK-UNK", 1001, orderflow.OrderStatus(99), time.Time{})
	ttl := server.TTL(c.key("TOK-UNK"))
	if ttl != 2*time.Minute {
		t.Errorf("unknown-status TTL = %s, want 2m", ttl)
	}
}

// =============================================================================
// Options 覆盖
// =============================================================================

func TestCache_WithCacheKeyPrefix(t *testing.T) {
	c, server, _ := newTestCache(t, WithCacheKeyPrefix("myapp:"))
	ctx := context.Background()
	_ = c.Set(ctx, "TOK-P", 1001, orderflow.StatusPaid, time.Time{})

	if _, err := server.Get("myapp:TOK-P"); err != nil {
		t.Fatalf("expected key under myapp: prefix, got: %v", err)
	}
}

func TestCache_WithTTLOverride(t *testing.T) {
	c, server, _ := newTestCache(t, WithTTL(orderflow.StatusPaid, 30*time.Second))
	ctx := context.Background()
	_ = c.Set(ctx, "TOK", 1001, orderflow.StatusPaid, time.Time{})

	ttl := server.TTL(c.key("TOK"))
	if ttl != 30*time.Second {
		t.Errorf("Paid TTL override = %s, want 30s", ttl)
	}
}

func TestCache_WithPendingGrace(t *testing.T) {
	c, server, _ := newTestCache(t, WithPendingGrace(time.Minute))
	ctx := context.Background()
	_ = c.Set(ctx, "TOK", 1001, orderflow.StatusPending, time.Now().Add(5*time.Minute))
	ttl := server.TTL(c.key("TOK"))
	// 5min expireAt + 1min grace ≈ 6min
	if ttl < 5*time.Minute+30*time.Second || ttl > 6*time.Minute+30*time.Second {
		t.Errorf("Pending TTL with 1min grace = %s, want ~6m", ttl)
	}
}

func TestCache_WithFallbackTTL(t *testing.T) {
	c, server, _ := newTestCache(t, WithFallbackTTL(10*time.Second))
	ctx := context.Background()
	_ = c.Set(ctx, "TOK", 1001, orderflow.OrderStatus(99), time.Time{})
	ttl := server.TTL(c.key("TOK"))
	if ttl != 10*time.Second {
		t.Errorf("fallback TTL override = %s, want 10s", ttl)
	}
}

// =============================================================================
// 编码 / 解码
// =============================================================================

func TestEncodeDecodeRoundtrip(t *testing.T) {
	cases := []struct {
		status orderflow.OrderStatus
		userID int64
	}{
		{orderflow.StatusPending, 1},
		{orderflow.StatusPaid, 123456789},
		{orderflow.StatusClosed, -1}, // 负值（理论上不会出现但防御性测试）
	}
	for _, c := range cases {
		raw := encodeCacheValue(c.status, c.userID)
		got, err := decodeCacheValue(raw)
		if err != nil {
			t.Fatalf("decode(%q): %v", raw, err)
		}
		if got.Status != c.status || got.UserID != c.userID {
			t.Errorf("roundtrip(%d, %d): got %+v", c.status, c.userID, got)
		}
	}
}

func TestDecodeCacheValue_RejectsMalformed(t *testing.T) {
	cases := []string{
		"",
		"no-colon",
		"abc:xyz",
		"1:notanumber",
	}
	for _, raw := range cases {
		if _, err := decodeCacheValue(raw); err == nil {
			t.Errorf("expected error for %q", raw)
		}
	}
}

func TestCache_NilClientReturnsError(t *testing.T) {
	c := NewStatusCache(nil)
	ctx := context.Background()

	check := func(name string, fn func() error) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("unexpected panic: %v", r)
					}
				}()
				err = fn()
			}()
			if err == nil {
				t.Fatal("expected explicit error for nil redis client")
			}
		})
	}

	check("Set", func() error {
		return c.Set(ctx, "TOK", 1001, orderflow.StatusPaid, time.Time{})
	})
	check("Get", func() error {
		_, _, err := c.Get(ctx, "TOK")
		return err
	})
	check("Delete", func() error {
		return c.Delete(ctx, "TOK")
	})
}
