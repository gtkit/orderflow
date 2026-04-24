package rediscache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderflow"
	"github.com/redis/go-redis/v9"
)

const defaultLockerKeyPrefix = "orderflow:lock:"

// releaseLockScript 是"CAS 释放锁"的 Lua 脚本：
// 只有 token 匹配才 DEL，防止节点 A 的 unlock 误删节点 B 刚获得的锁
// （场景：A 的锁 TTL 过期 → 被自动释放 → B 获得了同 key 的锁 → A 才跑 unlock → 会误删）。
var releaseLockScript = redis.NewScript(`
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		return redis.call('DEL', KEYS[1])
	end
	return 0
`)

// Locker 是 orderflow.Locker 的 Redis 实现。
// 基于 SET NX EX + Lua CAS 释放，符合 "Redlock-lite" 模式（单 Redis 实例，适合非 HA 场景；
// 多实例场景应考虑 redsync 或 Redis Sentinel 下的单 primary）。
type Locker struct {
	rdb       *redis.Client
	keyPrefix string
}

// LockerOption 配置 Locker。
type LockerOption func(*Locker)

// WithLockerKeyPrefix 覆盖锁 key 前缀（默认 "orderflow:lock:"）。
func WithLockerKeyPrefix(prefix string) LockerOption {
	return func(l *Locker) { l.keyPrefix = prefix }
}

// NewLocker 构造 Redis Locker。rdb 必填。
func NewLocker(rdb *redis.Client, opts ...LockerOption) *Locker {
	l := &Locker{
		rdb:       rdb,
		keyPrefix: defaultLockerKeyPrefix,
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

var _ orderflow.Locker = (*Locker)(nil)

var errNilOnPaidHook = errors.New("rediscache: on paid hook is nil")

// Validate reports whether the locker has all required internal dependencies.
func (l *Locker) Validate() error {
	if l == nil {
		return errors.New("rediscache: locker is nil")
	}
	if l.rdb == nil {
		return errNilRedisClient
	}
	return nil
}

// TryLock 非阻塞尝试获取锁。
//
// 实现：SET key token NX EX ttl。成功 → 持锁（返回 ok=true + 专属 unlock）。
// 失败（key 已存在）→ 返回 ok=false + no-op unlock。
//
// unlock 调用 Lua 脚本 CAS 删除：只在 GET(key) == token 时 DEL，避免误删其他客户端的锁。
func (l *Locker) TryLock(ctx context.Context, key string, ttl time.Duration) (func(), bool, error) {
	if err := l.Validate(); err != nil {
		return func() {}, false, err
	}
	token, err := generateToken()
	if err != nil {
		return func() {}, false, fmt.Errorf("rediscache/locker: generate token: %w", err)
	}
	fullKey := l.keyPrefix + key

	// SET key token NX EX ttl。NX 失败（key 已存在）会返回 redis.Nil，不是普通错误。
	_, err = l.rdb.SetArgs(ctx, fullKey, token, redis.SetArgs{
		Mode: "NX",
		TTL:  ttl,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// 锁被占——no-op unlock，ok=false
			return func() {}, false, nil
		}
		return func() {}, false, fmt.Errorf("rediscache/locker: SET NX: %w", err)
	}

	// 持锁成功，返回自己的 unlock
	unlock := func() {
		// 用独立 ctx，避免父 ctx 取消后 unlock 被跳过导致锁卡到 TTL 才释放
		rctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = releaseLockScript.Run(rctx, l.rdb, []string{fullKey}, token).Result()
	}
	return unlock, true, nil
}

// generateToken 生成 16 字节随机 token，作为锁的持有凭据。
func generateToken() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

// IdempotentOnPaidViaRedis 用 Redis SETNX 实现完整的幂等保护（无需业务侧 marker 表）。
//
// 语义：对同一 orderNo，inner 钩子至多被调用一次到成功完成。
//
// 实现：
//  1. SET NX EX token 作为幂等标记；
//  2. 成功（首次调用）→ 执行 inner；
//     - inner 返回 nil → 保留标记（TTL 期后自动清理）；
//     - inner 返回 error → 立即 DEL 标记让后续 fallback 可重入；
//  3. 失败（已有标记）→ 跳过 inner，返回 nil（视为"已处理"）。
//
// ttl 应覆盖业务可能重入的全部窗口（默认 24h 足以覆盖支付回调重试 + fallback scanner
// 周期）。过短会导致"标记过期 + 又收到重试" 的重复调用。
func IdempotentOnPaidViaRedis[O orderflow.OrderSnapshot](
	inner orderflow.OnPaidHook[O],
	rdb *redis.Client,
	keyPrefix string,
	ttl time.Duration,
) orderflow.OnPaidHook[O] {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if keyPrefix == "" {
		keyPrefix = "orderflow:onpaid:"
	}
	return func(ctx context.Context, o O, n orderflow.NotifyResult) error {
		if inner == nil {
			return errNilOnPaidHook
		}
		if rdb == nil {
			return errNilRedisClient
		}
		key := keyPrefix + o.OrderNo()
		// 尝试占位：SET NX EX。NX 失败（已存在）返回 redis.Nil，视为"已被处理过"。
		// marker value 携带 "<unix_ts>|<trade_no>"，方便排障：
		//   redis-cli get orderflow:onpaid:N20260424001 -> "1745467200|TXN-12345"
		// 历史值是 "1"，仍能正常读取（NX 模式下我们只关心是否已存在）。
		markerValue := fmt.Sprintf("%d|%s", time.Now().Unix(), n.TransactionID)
		_, err := rdb.SetArgs(ctx, key, markerValue, redis.SetArgs{
			Mode: "NX",
			TTL:  ttl,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// 已有标记，跳过
				return nil
			}
			return fmt.Errorf("rediscache/idempotent: SET NX: %w", err)
		}

		// 首次调用 inner；失败则删除标记允许重试
		if innerErr := inner(ctx, o, n); innerErr != nil {
			// 用独立 ctx 释放，避免 ctx 已取消时标记残留
			delCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if delErr := rdb.Del(delCtx, key).Err(); delErr != nil && !errors.Is(delErr, redis.Nil) {
				// 删除失败只 warn（不是 log 级别）；标记会 TTL 自动清理，代价是重试窗口推后
				return fmt.Errorf("rediscache/idempotent: inner failed and cleanup also failed (inner=%w, cleanup=%v)", innerErr, delErr)
			}
			return innerErr
		}
		return nil
	}
}
