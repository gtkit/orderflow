package orderflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Create 的交叉验证点：
//   - 返回值（Order 字段 + Reused + PaymentParams）
//   - Store：CreateCalls、AppendLogCalls、订单状态
//   - DelayQueue：EnqueueCalls、member 对齐订单号
//   - Cache：SetCalls、最终状态
//   - Gateway：UnifiedOrderCalls
//   - Hook：OnCreated / OnClosed / OnSuperseded 按场景触发
//
// 每个场景至少验证以上 6 个维度中的 4 个，以锁定真实行为而非局部副作用。

func TestCreate_HappyPath_NewOrder(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	result, err := env.engine.Create(ctx, standardRequest())
	mustNotErr(t, err, "Create")

	// 返回值
	mustEqual(t, result.Reused, false, "Reused")
	mustEqual(t, result.PaymentParams, "mock_pay_params", "PaymentParams")
	if result.Order.OrderNo() == "" {
		t.Fatal("Order.OrderNo is empty")
	}
	mustEqual(t, result.Order.Status(), StatusPending, "Order.Status")

	// Store
	mustEqual(t, env.store.CreateCalls, 1, "Store.CreateCalls")
	mustEqual(t, env.store.AppendLogCalls, 1, "Store.AppendLogCalls (created log)")
	mustLen(t, env.store.logs, 1, "Store.logs")
	mustEqual(t, env.store.logs[0].ToStatus, StatusPending, "log ToStatus")
	mustEqual(t, env.store.logs[0].Remark, "created", "log Remark")

	// DelayQueue
	mustEqual(t, env.dq.EnqueueCalls, 1, "DelayQueue.EnqueueCalls")
	if _, ok := env.dq.enqueued[result.Order.OrderNo()]; !ok {
		t.Fatalf("DelayQueue did not enqueue order %s", result.Order.OrderNo())
	}

	// Cache
	mustEqual(t, env.cache.SetCalls, 1, "Cache.SetCalls")
	mustLen(t, env.cache.SetHistory, 1, "Cache.SetHistory")
	mustEqual(t, env.cache.SetHistory[0].Status, StatusPending, "Cache first Set status")

	// Gateway
	mustEqual(t, env.gw.UnifiedOrderCalls, 1, "Gateway.UnifiedOrderCalls")

	// Hooks
	mustLen(t, env.OnCreatedCalls, 1, "OnCreated")
	mustLen(t, env.OnClosedCalls, 0, "OnClosed (should not fire)")
	mustLen(t, env.OnSupersededCalls, 0, "OnSuperseded (should not fire)")
}

func TestCreate_ReusePendingOrder(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	req := standardRequest()

	// Seed：已有一个匹配的 Pending 订单（价格 + 支付方式完全一致）
	existing := &testOrder{
		orderNo:       "EXISTING-001",
		orderToken:    "TOKEN-001",
		userID:        req.UserID,
		status:        StatusPending,
		productID:     req.Product.ID,
		productType:   req.Product.Type,
		productTitle:  req.Product.Title,
		originalPrice: req.Product.Price,
		payAmount:     req.Product.Price,
		payMethod:     req.PayMethod,
		expireAt:      time.Now().Add(time.Hour),
	}
	env.store.seed(existing)

	result, err := env.engine.Create(ctx, req)
	mustNotErr(t, err, "Create")

	// 应复用旧订单
	mustEqual(t, result.Reused, true, "Reused")
	mustEqual(t, result.Order.OrderNo(), "EXISTING-001", "Reused order no")
	mustEqual(t, result.PaymentParams, "mock_pay_params", "PaymentParams (refreshed)")

	// 副作用：不应新建订单、不应入延时队列、不应写新日志
	mustEqual(t, env.store.CreateCalls, 0, "Store.CreateCalls (no new create)")
	mustEqual(t, env.dq.EnqueueCalls, 0, "DelayQueue.EnqueueCalls (no new enqueue)")
	mustEqual(t, env.store.AppendLogCalls, 0, "AppendLog (no new log)")
	mustLen(t, env.OnCreatedCalls, 0, "OnCreated (reuse does not trigger)")

	// 应调用网关重新获取支付参数
	mustEqual(t, env.gw.UnifiedOrderCalls, 1, "Gateway.UnifiedOrderCalls (refresh params)")
}

func TestCreate_SupersedeWhenPayMethodDiffers(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	req := standardRequest()
	// 旧订单用微信，新请求用支付宝
	existing := &testOrder{
		orderNo:       "OLD-001",
		orderToken:    "TOKEN-OLD",
		userID:        req.UserID,
		status:        StatusPending,
		productID:     req.Product.ID,
		productType:   req.Product.Type,
		productTitle:  req.Product.Title,
		originalPrice: req.Product.Price,
		payAmount:     req.Product.Price,
		payMethod:     "wechat",
		expireAt:      time.Now().Add(time.Hour),
	}
	env.store.seed(existing)
	env.dq.enqueued[existing.orderNo] = existing.expireAt // 模拟旧单在延时队列中

	req.PayMethod = "alipay"
	result, err := env.engine.Create(ctx, req)
	mustNotErr(t, err, "Create")

	// 应该返回新订单（不是 Reused）
	mustEqual(t, result.Reused, false, "Reused")
	if result.Order.OrderNo() == existing.orderNo {
		t.Fatalf("got same order no as superseded, expected new")
	}
	mustEqual(t, result.Order.Status(), StatusPending, "new order.Status")

	// 旧订单已关闭
	mustEqual(t, existing.status, StatusClosed, "old order status")

	// 网关：应该有 1 次 CloseOrder（关旧单）+ 1 次 UnifiedOrder（下新单）
	mustEqual(t, env.gw.CloseOrderCalls, 1, "Gateway.CloseOrderCalls")
	mustEqual(t, env.gw.UnifiedOrderCalls, 1, "Gateway.UnifiedOrderCalls")

	// DelayQueue：旧单被 Remove，新单被 Enqueue
	if env.dq.RemoveCalls < 1 {
		t.Fatalf("expected delay queue Remove call, got %d", env.dq.RemoveCalls)
	}
	mustEqual(t, env.dq.EnqueueCalls, 1, "new Enqueue")

	// Hook：OnSuperseded + OnClosed(Superseded) + OnCreated 均触发
	mustLen(t, env.OnSupersededCalls, 1, "OnSuperseded")
	mustEqual(t, env.OnSupersededCalls[0].OldOrderNo, existing.orderNo, "OnSuperseded oldOrderNo")
	mustEqual(t, env.OnSupersededCalls[0].NewProductID, req.Product.ID, "OnSuperseded newProductID")
	mustLen(t, env.OnClosedCalls, 1, "OnClosed")
	mustEqual(t, env.OnClosedCalls[0].Reason, ClosedReasonSuperseded, "OnClosed reason")
	mustLen(t, env.OnCreatedCalls, 1, "OnCreated (new order)")

	// Stream：旧单 Closed 会 publish；新订单创建只写 cache 不推 stream
	// （新订单此时不会有订阅者，publish 多余）。所以只预期 1 次 publish。
	mustLen(t, env.stream.Published, 1, "Published events")
	mustEqual(t, env.stream.Published[0].Status, StatusClosed, "published status")
	mustEqual(t, env.stream.Published[0].OrderToken, existing.orderToken, "published token")
}

func TestCreate_SupersedeRaceLostToPayment(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	req := standardRequest()
	existing := &testOrder{
		orderNo:       "OLD-002",
		orderToken:    "TOKEN-OLD-2",
		userID:        req.UserID,
		status:        StatusPending,
		productID:     req.Product.ID,
		originalPrice: req.Product.Price,
		payAmount:     req.Product.Price,
		payMethod:     "wechat",
		expireAt:      time.Now().Add(time.Hour),
	}
	env.store.seed(existing)

	// 模拟真实竞态：existing 目前是 Pending（所以 FindPendingByUserAndProduct 能命中），
	// 但 CASClose 执行时被支付回调抢先推进成 Paid。
	// CASCloseLosesToPaidOnce 会让第一次 CASClose 返回 0 并同时把 order 改成 Paid。
	env.store.CASCloseLosesToPaidOnce = true

	req.PayMethod = "alipay"
	result, err := env.engine.Create(ctx, req)
	mustNotErr(t, err, "Create")

	// 应该返回 current（Paid 旧单）+ Reused=true，让客户端认这个单
	mustEqual(t, result.Reused, true, "Reused should be true (current order won race)")
	mustEqual(t, result.Order.OrderNo(), existing.orderNo, "returned order no")
	mustEqual(t, result.Order.Status(), StatusPaid, "returned order status")
	mustEqual(t, result.PaymentParams, "", "PaymentParams empty (no new payment request)")

	// 不应创建新订单
	mustEqual(t, env.store.CreateCalls, 0, "Store.CreateCalls")
	mustEqual(t, env.dq.EnqueueCalls, 0, "DelayQueue.EnqueueCalls")
	mustLen(t, env.OnCreatedCalls, 0, "OnCreated")
}

func TestCreate_EnqueueFailureRollsBackToClosed(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	env.dq.ErrOnEnqueue = errors.New("redis down")

	result, err := env.engine.Create(ctx, standardRequest())
	if err == nil {
		t.Fatalf("expected error from Create, got nil (result=%+v)", result)
	}

	// 订单已创建但又被回滚关闭
	mustEqual(t, env.store.CreateCalls, 1, "Store.CreateCalls")
	mustEqual(t, env.store.CASCloseCalls, 1, "Store.CASCloseCalls (rollback)")

	// 唯一的订单应处于 Closed 状态
	mustLen(t, env.OnClosedCalls, 1, "OnClosed (enqueue_fail)")
	mustEqual(t, env.OnClosedCalls[0].Reason, ClosedReasonEnqueueFail, "OnClosed reason")

	// Cache 应写了 Closed
	found := false
	for _, ev := range env.cache.SetHistory {
		if ev.Status == StatusClosed {
			found = true
		}
	}
	if !found {
		t.Fatalf("cache should have Closed event after rollback, got %+v", env.cache.SetHistory)
	}
}

func TestCreate_RejectsInvalidInput(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	cases := []struct {
		name    string
		mutator func(*CreateRequest)
	}{
		{"UserID 0", func(r *CreateRequest) { r.UserID = 0 }},
		{"Product.ID 0", func(r *CreateRequest) { r.Product.ID = 0 }},
		{"Product.Title empty", func(r *CreateRequest) { r.Product.Title = "" }},
		{"Product.Title too long", func(r *CreateRequest) {
			r.Product.Title = stringOfLen(maxCreateProductTitleLen + 1)
		}},
		{"Product.Type too long", func(r *CreateRequest) {
			r.Product.Type = stringOfLen(maxCreateProductTypeLen + 1)
		}},
		{"PayMethod empty", func(r *CreateRequest) { r.PayMethod = "" }},
		{"PayMethod too long", func(r *CreateRequest) {
			r.PayMethod = stringOfLen(maxCreatePayMethodLen + 1)
		}},
		{"ClientIP not a valid IP", func(r *CreateRequest) { r.ClientIP = "not-an-ip" }},
		{"ClientIP with CRLF injection", func(r *CreateRequest) {
			r.ClientIP = "127.0.0.1\r\nfake log entry"
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := standardRequest()
			c.mutator(&req)
			_, err := env.engine.Create(ctx, req)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("expected ErrInvalidConfig, got %v", err)
			}
		})
	}

	// 无效输入不应有任何副作用
	mustEqual(t, env.store.CreateCalls, 0, "no create")
	mustEqual(t, env.dq.EnqueueCalls, 0, "no enqueue")
}

func stringOfLen(n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = 'x'
	}
	return string(buf)
}
