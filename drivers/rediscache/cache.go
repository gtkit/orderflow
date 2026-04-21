package rediscache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gtkit/orderflow"
	"github.com/redis/go-redis/v9"
)

const (
	defaultCacheKeyPrefix = "orderflow:status:"
	defaultPendingGrace   = 5 * time.Minute
	defaultFallbackTTL    = 2 * time.Minute
	defaultActiveTTL      = 5 * time.Minute
	defaultTerminalTTL    = 2 * time.Minute
)

// StatusCache 是基于 Redis 的 orderflow.StatusCache 实现。
type StatusCache struct {
	rdb          *redis.Client
	keyPrefix    string
	ttls         map[orderflow.OrderStatus]time.Duration
	pendingGrace time.Duration
	fallbackTTL  time.Duration
}

// CacheOption 是 NewStatusCache 的可选配置。
type CacheOption func(*StatusCache)

// WithCacheKeyPrefix 覆盖缓存 key 前缀（默认 "orderflow:status:"）。
func WithCacheKeyPrefix(prefix string) CacheOption {
	return func(c *StatusCache) { c.keyPrefix = prefix }
}

// WithPendingGrace 覆盖 Pending 态在订单过期时间之后额外保留的 TTL（默认 5min）。
// 主要用于让 APP 轮询到最终的 Closed 状态再跳转，避免卡在"最后一刻 Pending"。
func WithPendingGrace(d time.Duration) CacheOption {
	return func(c *StatusCache) { c.pendingGrace = d }
}

// WithFallbackTTL 覆盖未知状态 / 过期兜底的 TTL（默认 2min）。
func WithFallbackTTL(d time.Duration) CacheOption {
	return func(c *StatusCache) { c.fallbackTTL = d }
}

// WithTTL 针对指定状态覆盖 TTL。
// 多次调用可单独覆盖多个状态；未覆盖的状态继续使用默认。
func WithTTL(status orderflow.OrderStatus, d time.Duration) CacheOption {
	return func(c *StatusCache) { c.ttls[status] = d }
}

// NewStatusCache 构造 StatusCache。rdb 必填；为 nil 时后续方法会发生 nil 解引用 panic。
func NewStatusCache(rdb *redis.Client, opts ...CacheOption) *StatusCache {
	c := &StatusCache{
		rdb:          rdb,
		keyPrefix:    defaultCacheKeyPrefix,
		ttls:         defaultStatusTTLs(),
		pendingGrace: defaultPendingGrace,
		fallbackTTL:  defaultFallbackTTL,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

var _ orderflow.StatusCache = (*StatusCache)(nil)

// Set 写入状态缓存。
// expireAt 只在 status == StatusPending 时使用，其它状态传零值即可。
func (c *StatusCache) Set(ctx context.Context, orderToken string, userID int64, status orderflow.OrderStatus, expireAt time.Time) error {
	if err := c.rdb.Set(ctx, c.key(orderToken), encodeCacheValue(status, userID), c.ttlFor(status, expireAt)).Err(); err != nil {
		return fmt.Errorf("rediscache: set: %w", err)
	}
	return nil
}

// Get 读取状态缓存。
// 三元返回：
//   - cached, true, nil   → 命中
//   - {}, false, nil      → miss（含脏数据）
//   - {}, false, err      → Redis 错误
func (c *StatusCache) Get(ctx context.Context, orderToken string) (orderflow.CachedStatus, bool, error) {
	raw, err := c.rdb.Get(ctx, c.key(orderToken)).Result()
	if errors.Is(err, redis.Nil) {
		return orderflow.CachedStatus{}, false, nil
	}
	if err != nil {
		return orderflow.CachedStatus{}, false, fmt.Errorf("rediscache: get: %w", err)
	}
	cached, err := decodeCacheValue(raw)
	if err != nil {
		// 旧格式或脏数据按 miss 处理，上层会回源后用新格式覆盖。
		return orderflow.CachedStatus{}, false, nil
	}
	return cached, true, nil
}

// Delete 删除状态缓存。
func (c *StatusCache) Delete(ctx context.Context, orderToken string) error {
	if err := c.rdb.Del(ctx, c.key(orderToken)).Err(); err != nil {
		return fmt.Errorf("rediscache: delete: %w", err)
	}
	return nil
}

func (c *StatusCache) key(orderToken string) string {
	return c.keyPrefix + orderToken
}

func (c *StatusCache) ttlFor(status orderflow.OrderStatus, expireAt time.Time) time.Duration {
	if status == orderflow.StatusPending {
		ttl := time.Until(expireAt) + c.pendingGrace
		if ttl <= 0 {
			ttl = c.fallbackTTL
		}
		return ttl
	}
	if ttl, ok := c.ttls[status]; ok {
		return ttl
	}
	return c.fallbackTTL
}

// defaultStatusTTLs 返回沿用 sleep_client 现网策略的默认 TTL 表。
func defaultStatusTTLs() map[orderflow.OrderStatus]time.Duration {
	return map[orderflow.OrderStatus]time.Duration{
		orderflow.StatusPaid:      defaultActiveTTL,
		orderflow.StatusDelivered: defaultActiveTTL,
		orderflow.StatusCompleted: defaultActiveTTL,
		orderflow.StatusClosed:    defaultTerminalTTL,
		orderflow.StatusCancelled: defaultTerminalTTL,
	}
}

// encodeCacheValue 编码为 "<status>:<user_id>"。
// 选紧凑字符串而非 JSON：典型值 < 10 字节；解码只需一次 strings.Cut + 两次 strconv 解析。
func encodeCacheValue(status orderflow.OrderStatus, userID int64) string {
	return strconv.Itoa(int(status)) + ":" + strconv.FormatInt(userID, 10)
}

// decodeCacheValue 反序列化缓存值。格式非法时返回 error，调用方按 miss 处理。
func decodeCacheValue(raw string) (orderflow.CachedStatus, error) {
	statusPart, userPart, ok := strings.Cut(raw, ":")
	if !ok {
		return orderflow.CachedStatus{}, fmt.Errorf("malformed cache value: %q", raw)
	}
	status, err := strconv.Atoi(statusPart)
	if err != nil {
		return orderflow.CachedStatus{}, fmt.Errorf("parse status: %w", err)
	}
	userID, err := strconv.ParseInt(userPart, 10, 64)
	if err != nil {
		return orderflow.CachedStatus{}, fmt.Errorf("parse user id: %w", err)
	}
	return orderflow.CachedStatus{
		Status: orderflow.OrderStatus(status),
		UserID: userID,
	}, nil
}
