// Package rediscache 用 go-redis 实现 orderflow.StatusCache 与 orderflow.StatusStream。
//
// 设计取舍：
//
//   - 缓存值采用紧凑字符串 "<status>:<user_id>"，既不引入 JSON 依赖，也在 key/value 都短小的情况下
//     比 JSON 省约 3/4 的空间（典型 8 字节 vs 28 字节）。
//   - TTL 按订单状态派发：Pending 对齐订单过期时间 + PendingGrace；Closed/Cancelled 较短（默认 2min）；
//     其他活跃状态默认 5min。全部可通过 WithTTL / WithPendingGrace / WithFallbackTTL 覆盖。
//   - StatusStream 基于 Redis Pub/Sub，消息只承载状态整数；订阅方会启动一个 forward goroutine
//     把 redis.Message 转成 orderflow.OrderStatus 通道。
//
// 使用示例：
//
//	cache  := rediscache.NewStatusCache(rdb)
//	stream := rediscache.NewStatusStream(rdb)
//
//	engine, _ := orderflow.New[MyOrder](orderflow.Config[MyOrder]{
//	    Cache:  cache,
//	    Stream: stream,
//	    // ...其他能力接口
//	})
package rediscache
