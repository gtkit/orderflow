// Package rediszq 用 Redis ZSET 实现 orderflow.DelayQueue。
//
// 语义：
//
//   - pending ZSET：待执行队列，score = executeAt 毫秒时间戳；Enqueue 时用 NX 语义天然去重。
//   - processing ZSET：已预留但未确认的队列，score = lease 到期时间；
//     Ack 成功即删除，未 Ack 且 lease 过期可被 RequeueExpired 拉回 pending。
//   - Lua 脚本确保 "ZRANGEBYSCORE + ZREM / ZADD" 在 Redis 单线程内原子完成，多实例消费天然安全。
//
// 使用示例：
//
//	q, err := rediszq.New(rdb, "myapp:order:delay_close")
//	if err != nil {
//	    return err
//	}
//
// **Redis 集群部署注意**：Queue 内部同时操作 `key` 和 `key + ":processing"` 两个 key，
// 必须通过 hash tag 约束到同一 slot 才能被同一 Lua 脚本处理，否则 `CROSSSLOT` 错误会
// 让 ReserveExpired / RequeueExpired / Remove 全部失败。
//
// 集群部署时传入带 hash tag 的 key：
//
//	q, err := rediszq.New(rdb, "{myapp}:order:delay_close")
//	// pending key:    {myapp}:order:delay_close
//	// processing key: {myapp}:order:delay_close:processing
//	// 两个 key 都 hash 到 "myapp" 这个 tag 的 slot。
//
// 单机 / 哨兵部署不受此限制，任何 key 都可以。
//
//	engine, _ := orderflow.New[MyOrder](orderflow.Config[MyOrder]{
//	    DelayQueue: q,
//	    // ...其他能力接口
//	})
//
// 本包代码从 sleep_client 生产项目的 internal/pkg/delayqueue 迁移而来，经过多轮现网运行验证。
package rediszq
