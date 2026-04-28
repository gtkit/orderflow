package orderflow

import (
	"context"
	"time"
)

// StatusCache 为轮询客户端（APP polling）提供订单状态的快速读路径。
//
// 承诺：所有读取都可以直接命中缓存而不回源 DB；TTL 由实现层根据状态推导，
// 非终态可以短 TTL，终态可以长 TTL（例如 24h），由 driver 决定。
//
// # Token 撤销 / 风控止血（业务方使用）
//
// `OrderToken` 默认与订单生命周期等长，没有内置失效机制。当业务方检测到 token
// 异常访问（IP 漂移、客户端工单、风控告警）需要主动止血时，可以直接调用：
//
//	cache.Delete(ctx, orderToken)        // 让 PollStatus 立即 miss
//	store.UpdateByOrderNo(ctx, orderNo, ...)  // 如需持久化撤销标记由业务方扩展
//
// 删除缓存后下一次 PollStatus 会回源 DB，业务方在自己的 Store 实现里维护一张
// "revoked tokens" 黑名单（或加一个状态列）即可让回源也返回 ErrOrderForbidden。
// orderflow 库本身不内置黑名单——保持中立，把策略交给业务侧。
type StatusCache interface {
	Set(ctx context.Context, orderToken string, userID int64, status OrderStatus, expireAt time.Time) error
	Get(ctx context.Context, orderToken string) (CachedStatus, bool, error)
	// Delete 删除缓存项。除了 Engine 内部的状态推送一致性使用外，业务方也可以
	// 主动调用本方法做 token 撤销 / 风控止血——见接口文档"Token 撤销"章节。
	Delete(ctx context.Context, orderToken string) error
}

// CachedStatus 是 StatusCache 返回的缓存值。
// 携带 UserID 以便在 Engine.PollStatus 中完成归属校验，避免回源 DB。
type CachedStatus struct {
	UserID int64
	Status OrderStatus
}

// StatusStream 为订阅客户端（SSE / WebSocket）提供订单状态的实时推送通道。
//
// Publish 在状态发生变化后推送一次（由 Engine 在关键跃迁点调用）；Subscribe 返回
// 一个可消费的 Subscription，订阅方应在 ctx 结束或 Close 后丢弃。
type StatusStream interface {
	Publish(ctx context.Context, orderToken string, status OrderStatus) error
	Subscribe(ctx context.Context, orderToken string) (Subscription, error)
}

// Subscription 承载单个 orderToken 的状态变更流。
type Subscription interface {
	// Events 返回一个只读 channel，driver 关闭 channel 表示订阅结束。
	Events() <-chan OrderStatus
	// Close 由调用方显式终止订阅；必须幂等。
	Close() error
}
