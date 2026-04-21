package orderflow

import (
	"context"
	"time"
)

// StatusCache 为轮询客户端（APP polling）提供订单状态的快速读路径。
//
// 承诺：所有读取都可以直接命中缓存而不回源 DB；TTL 由实现层根据状态推导，
// 非终态可以短 TTL，终态可以长 TTL（例如 24h），由 driver 决定。
type StatusCache interface {
	Set(ctx context.Context, orderToken string, userID int64, status OrderStatus, expireAt time.Time) error
	Get(ctx context.Context, orderToken string) (CachedStatus, bool, error)
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
