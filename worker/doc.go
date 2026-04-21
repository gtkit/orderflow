// Package worker 提供 orderflow 的后台 worker，消费延时队列并兜底补偿异常订单。
//
// 三个 worker 各司其职：
//
//   - CloseWorker：从 DelayQueue 取出到期任务，调用 Engine.Close 推进状态。
//   - CloseFallback：周期扫描 DB 中过期但未被关闭的 Pending 订单（CloseWorker 漏掉或 Redis 丢数据时兜底）。
//   - DeliveryFallback：周期扫描 Paid 但未 Delivered 的订单，调用 Engine.ReconcilePaid 补偿。
//
// 生产环境通过 StartAll 一次性拉起三个 worker，阻塞在 ctx.Done 上优雅退出。
package worker
