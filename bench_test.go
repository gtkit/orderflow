package orderflow

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// 这一组 benchmark 覆盖 Engine 最常被调用的热路径：Create / HandleNotify /
// PollStatus。全部使用内存 fakes（fakeStore / fakeCache / fakeStream / fakeGateway /
// fakeDelayQueue），所以测到的是 Engine 编排开销 + 锁、map 操作、hook 调用成本，
// 不包含 DB / Redis / HTTP 的真实 I/O。
//
// 目的：
//   1. 建立性能回归基线——每次重构后 -count=3 跑一遍，对比 ns/op、allocs/op；
//   2. 辅助排查分配点——b.ReportAllocs() 强制开启 allocs/op 统计。
//
// 使用：
//   go test -bench=. -benchmem -count=3 .

func BenchmarkEngine_Create(b *testing.B) {
	env := newTestEnv(b)
	ctx := context.Background()

	base := standardRequest()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		req := base
		// 每次用不同的 product id 规避 FindPendingByUserAndProduct 的复用命中分支，
		// 确保走完整 Create 路径。
		req.Product.ID = uint64(2001 + i)
		if _, err := env.engine.Create(ctx, req); err != nil {
			b.Fatalf("Create: %v", err)
		}
	}
}

// BenchmarkEngine_HandleNotify_PendingToPaid 覆盖标准支付回调路径：
// Pending → CASConfirmPaid → OnPaid → FinalizePaidOrder → Delivered。
func BenchmarkEngine_HandleNotify_PendingToPaid(b *testing.B) {
	env := newTestEnv(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// 每轮 seed 新订单 + 准备 notify，把 HandleNotify 之外的 setup 成本隔离
		b.StopTimer()
		orderNo := fmt.Sprintf("BN-%d", i)
		seedBenchPendingOrder(env, orderNo)
		env.gw.NotifyResult = NotifyResult{
			OutTradeNo:    orderNo,
			TransactionID: "TXN-" + orderNo,
			TradeStatus:   TradeStatusPaid,
			TotalAmount:   9900,
			PaidAt:        time.Now(),
			Channel:       "wechat",
		}
		b.StartTimer()

		if err := env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()); err != nil {
			b.Fatalf("HandleNotify: %v", err)
		}
	}
}

// BenchmarkEngine_PollStatus_CacheHit 覆盖缓存命中的读路径——Cache.Get 直接返回，
// 不回源 DB，这是生产最高频的 API（APP 轮询）。
func BenchmarkEngine_PollStatus_CacheHit(b *testing.B) {
	env := newTestEnv(b)
	ctx := context.Background()

	o := seedBenchPendingOrder(env, "BP-1")
	// 预热缓存
	if err := env.cache.Set(ctx, o.OrderToken(), o.UserID(), o.Status(), o.ExpireAt()); err != nil {
		b.Fatalf("cache seed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := env.engine.PollStatus(ctx, o.OrderToken(), o.UserID()); err != nil {
			b.Fatalf("PollStatus: %v", err)
		}
	}
}

// BenchmarkEngine_PollStatus_CacheMiss 覆盖缓存未命中回源 DB 的慢路径。
// 两条路径的 ns/op 差异直接反映缓存命中的收益。
func BenchmarkEngine_PollStatus_CacheMiss(b *testing.B) {
	env := newTestEnv(b)
	ctx := context.Background()

	o := seedBenchPendingOrder(env, "BPM-1")
	// 不预热缓存——每次查询都回源 store + 回填

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		// 每次都先删缓存保证 miss
		b.StopTimer()
		_ = env.cache.Delete(ctx, o.OrderToken())
		b.StartTimer()

		if _, err := env.engine.PollStatus(ctx, o.OrderToken(), o.UserID()); err != nil {
			b.Fatalf("PollStatus: %v", err)
		}
	}
}

// seedBenchPendingOrder 是 benchmark 专用的 Pending 订单 seed，和 seedPendingOrder 几乎一样
// 但不依赖 *testing.T；返回的 order 已 seed 到 env.store。
func seedBenchPendingOrder(env *testEnv, orderNo string) *testOrder {
	o := &testOrder{
		orderNo:       orderNo,
		orderToken:    "TOK-" + orderNo,
		userID:        1001,
		status:        StatusPending,
		productID:     2001,
		productTitle:  "VIP",
		originalPrice: 9900,
		payAmount:     9900,
		payMethod:     "wechat",
		expireAt:      time.Now().Add(time.Hour),
	}
	env.store.seed(o)
	return o
}
