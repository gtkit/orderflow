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
	// Set 写入或刷新缓存项。
	//
	// **TTL 派生约定**：
	//   - 当 status == StatusPending 时，driver 应基于 expireAt 派生 TTL
	//     （典型：time.Until(expireAt) + 一段 grace 期）；
	//   - 其它状态（Paid / Delivered / Closed / Cancelled / Completed）应使用
	//     driver 内部的固定 TTL 表，**不依赖** expireAt。
	//
	// **负 TTL 防御契约**：当 status == StatusPending 且 expireAt 已过去（例如测试场景
	// 直接 seed 过期订单、调度延迟导致 Set 调用相对 expireAt 滞后）时，driver **必须**
	// clamp TTL 为正值（典型做法：fallback 到一个 driver 内部的最小 TTL），**不得**把
	// 负 TTL 透传给底层（Redis SET 负 EX 会报 ERR invalid expire time）。drivers/rediscache
	// 已实现该 clamp，自研 driver 必须遵循。
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
//
// # 交付语义（核心包不保证 at-least-once）
//
// 核心包**不**承诺事件 at-least-once 交付：典型 driver（如 drivers/rediscache 的
// Pub/Sub 实现）在客户端断线 → 重连的窗口内推送的状态变更**会被丢弃**，Redis
// Pub/Sub 不为离线订阅者缓存消息。新增的高级实现（Streams / Kafka 类）即使提供
// at-least-once 也属于 driver 增量能力，不是接口承诺。
//
// **客户端实现策略**：长连接 + 短轮询双路设计，把 Subscription 当作 push fast-path、
// PollStatus 当作 pull 兜底——周期（典型 10-30s）调一次 PollStatus 即可覆盖断线
// 期间的丢失事件。**不要**仅依赖 Subscription——你会丢状态。
//
// driver 实装可在自己的 README 写明具体语义（例如 rediscache 必须文档化"基于 Redis
// Pub/Sub 实现，无离线消息缓存"）。
type Subscription interface {
	// Events 返回一个只读 channel，driver 关闭 channel 表示订阅结束。
	Events() <-chan OrderStatus
	// Close 由调用方显式终止订阅；必须幂等。
	Close() error
}
