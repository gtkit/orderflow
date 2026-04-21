package orderflow

import (
	"context"
	"time"
)

// Locker 是分布式互斥锁的抽象。
//
// 使用场景：Engine.Create 在注入 Locker 后会对 (user_id, product_id) 维度串行化，
// 避免并发下单产生多个 Pending 订单（例如用户快速点击"支付"按钮，或多个 API 客户端
// 并发调用）。
//
// 接口语义：
//   - TryLock 尝试**非阻塞**获取锁。ok=true 表示获取成功；ok=false 表示被占，
//     调用方应视为并发冲突（Engine 返回 ErrConcurrentCreate）。
//   - unlock 函数在 ok=true / ok=false / err != nil 三种情况下都返回，调用方可
//     defer unlock() 而不必分支。ok=false 时 unlock 必须是 no-op。
//   - ttl 是锁的有效期：即便持锁进程崩溃，其他客户端最多等 ttl 后可重新获取。
//   - 实现必须保证 unlock 只释放自己持有的锁——用唯一 token + Lua 脚本做 CAS 释放，
//     否则节点 A 的 unlock 可能误删节点 B 刚持的锁。
type Locker interface {
	TryLock(ctx context.Context, key string, ttl time.Duration) (unlock func(), ok bool, err error)
}

// IdempotentOnPaid 包装 OnPaid 钩子，用外部 marker 实现"至多调用一次成功"的幂等。
//
// 工作原理：调 inner 前先查 markerExists(orderNo)；已存在则跳过（视为业务已处理）；
// 不存在则调 inner。由业务方负责在 inner 成功路径里写入 marker。
//
// marker 可以是：
//   - **业务自有的幂等表**（推荐）：在 inner 开头 `INSERT IGNORE INTO order_grants (order_no) ...`，
//     同一事务内做权益发放。markerExists 查询该表。
//   - **Engine 的 bill 表**（有前提）：仅当业务方保证"OnPaid 成功 → bill 一定写入"时
//     才安全。当前 Engine 时序是 OnPaid 先 FinalizePaidOrder 后，所以 "OnPaid 成功
//     但 Finalize 失败" 的窗口期 bill 还不存在——此窗口重试会再次调 OnPaid，业务方
//     如果不在 inner 内部做二级幂等，会重复发放。因此不建议用 bill 作为 marker，
//     除非业务 OnPaid 实现本身就幂等。
//   - **Redis 键**（见 drivers/rediscache/IdempotentOnPaidViaRedis）：Engine 无关，
//     适合业务不想改 DB schema 的场景。
//
// 契约：
//   - markerExists 查询失败时，inner 不会被调用，本函数返回错误让 fallback 重试。
//   - markerExists 返回 exists=true 时视为"已处理成功"，返回 nil。调用方不应在此
//     做额外断言——如果要区分"真的成功过"还是"marker 被错误设置"，应扩展接口。
func IdempotentOnPaid[O OrderSnapshot](
	inner OnPaidHook[O],
	markerExists func(ctx context.Context, orderNo string) (bool, error),
) OnPaidHook[O] {
	return func(ctx context.Context, o O, n NotifyResult) error {
		exists, err := markerExists(ctx, o.OrderNo())
		if err != nil {
			return err
		}
		if exists {
			// 已处理过，幂等跳过
			return nil
		}
		return inner(ctx, o, n)
	}
}
