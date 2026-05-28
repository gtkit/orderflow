package orderflow

import (
	"context"
	"errors"
	"strings"
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

func TestCreate_CustomGenerateOrderNoReceivesUserIDAndPrefixesDefault(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	gw := newFakeGateway()
	dq := newFakeDelayQueue()
	cache := newFakeCache()
	stream := newFakeStream()

	var gotUserID int64
	var generateCalls int
	engine, err := New[*testOrder](Config[*testOrder]{
		Store:      store,
		Gateway:    gw,
		DelayQueue: dq,
		Cache:      cache,
		Stream:     stream,
		Logger:     nopLogger{},
		GenerateOrderNo: func(userID int64) string {
			generateCalls++
			gotUserID = userID
			return "BIZ" + DefaultGenerateOrderNo(userID)
		},
	})
	mustNotErr(t, err, "New")

	req := standardRequest()
	req.UserID = 987654
	result, err := engine.Create(ctx, req)
	mustNotErr(t, err, "Create")

	mustEqual(t, generateCalls, 1, "GenerateOrderNo calls")
	mustEqual(t, gotUserID, req.UserID, "GenerateOrderNo userID")
	if !strings.HasPrefix(result.Order.OrderNo(), "BIZ") {
		t.Fatalf("order no %q should have business prefix", result.Order.OrderNo())
	}
	mustEqual(t, len(result.Order.OrderNo()), 33, "prefixed order no length")

	created, found, err := store.GetByNo(ctx, result.Order.OrderNo())
	mustNotErr(t, err, "Store.GetByNo")
	if !found {
		t.Fatalf("created order %q not found in store", result.Order.OrderNo())
	}
	mustEqual(t, created.UserID(), req.UserID, "created order userID")
	mustEqual(t, store.CreateCalls, 1, "Store.CreateCalls")
	mustEqual(t, gw.UnifiedOrderCalls, 1, "Gateway.UnifiedOrderCalls")
	mustEqual(t, dq.EnqueueCalls, 1, "DelayQueue.EnqueueCalls")
	mustEqual(t, cache.SetCalls, 1, "Cache.SetCalls")
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
		payMethod:     PayMethodWechat,
		expireAt:      time.Now().Add(time.Hour),
	}
	env.store.seed(existing)
	env.dq.enqueued[existing.orderNo] = existing.expireAt // 模拟旧单在延时队列中

	req.PayMethod = PayMethodAlipay
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
		payMethod:     PayMethodWechat,
		expireAt:      time.Now().Add(time.Hour),
	}
	env.store.seed(existing)

	// 模拟真实竞态：existing 目前是 Pending（所以 FindPendingByUserAndProduct 能命中），
	// 但 CASClose 执行时被支付回调抢先推进成 Paid。
	// CASCloseLosesToPaidOnce 会让第一次 CASClose 返回 0 并同时把 order 改成 Paid。
	env.store.CASCloseLosesToPaidOnce = true

	req.PayMethod = PayMethodAlipay
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

	// rollback 路径不应触发任何业务钩子：订单从未对业务侧可见（OnCreated 也未被调用）
	// 触发 OnClosed 会让事件总线收到一个"凭空冒出来的关闭事件"，破坏事件序列对称性。
	mustLen(t, env.OnCreatedCalls, 0, "OnCreated should not fire on enqueue rollback")
	mustLen(t, env.OnClosedCalls, 0, "OnClosed should not fire on enqueue rollback (no matching OnCreated)")

	// Cache 应写了 Closed（cache/observer 是基础设施层，与业务钩子分离）
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
		{"PayMethod zero", func(r *CreateRequest) { r.PayMethod = 0 }},
		{"Product.Price zero", func(r *CreateRequest) { r.Product.Price = 0 }},
		{"Product.Price negative", func(r *CreateRequest) { r.Product.Price = -1 }},
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

// seedSupersedeOldOrder 注入一个同用户、同商品但支付方式不同的旧 Pending 单，
// 用来触发 Engine.Create 的 closeSuperseded 分支。
func seedSupersedeOldOrder(t *testing.T, env *testEnv, req CreateRequest) *testOrder {
	t.Helper()
	old := &testOrder{
		orderNo:       "OLD-SUP",
		orderToken:    "TOKEN-OLD-SUP",
		userID:        req.UserID,
		status:        StatusPending,
		productID:     req.Product.ID,
		productType:   req.Product.Type,
		productTitle:  req.Product.Title,
		originalPrice: req.Product.Price,
		payAmount:     req.Product.Price,
		payMethod:     PayMethodWechat, // 与 standardRequest 默认不同
		expireAt:      time.Now().Add(time.Hour),
	}
	env.store.seed(old)
	env.dq.enqueued[old.orderNo] = old.expireAt
	return old
}

// TestCreate_SupersedeStrict_GatewayCloseFailureBlocks 验证默认 SupersededStrict 行为：
// 网关 CloseOrder 失败 → closeSuperseded 直接 return error → Create 失败 → 用户被阻塞下单。
// 这是 v1.0.0 默认行为，需要保持向后兼容。
func TestCreate_SupersedeStrict_GatewayCloseFailureBlocks(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// 默认 policy 是 SupersededStrict（零值），不显式设置
	req := standardRequest()
	req.PayMethod = PayMethodAlipay // 触发 superseded（旧单是 wechat）
	old := seedSupersedeOldOrder(t, env, req)

	// 网关 CloseOrder 注入持久错误
	env.gw.CloseOrderErr = errors.New("gateway 5xx")

	_, err := env.engine.Create(ctx, req)
	if err == nil {
		t.Fatal("expected error in SupersededStrict mode when gateway fails")
	}
	if !errContains(err, "close superseded") {
		t.Errorf("err = %v, want wrap of 'close superseded'", err)
	}

	// 旧单状态不变（未被 CAS Close），新单未创建
	mustEqual(t, old.status, StatusPending, "old order status unchanged")
	mustEqual(t, env.store.CreateCalls, 0, "no new order created")
	mustEqual(t, env.gw.UnifiedOrderCalls, 0, "no UnifiedOrder for new")
}

// TestCreate_SupersedeDegraded_GatewayCloseFailureContinues 验证 SupersededDegraded：
// 网关 CloseOrder 失败 → 记 ALERT 日志 + 继续走本地 CAS Close + Create 新单成功。
// 推荐生产配置：网关偶发抖动不应阻塞用户下新单。
func TestCreate_SupersedeDegraded_GatewayCloseFailureContinues(t *testing.T) {
	env := newTestEnv(t)
	env.engine.closeSupersededPolicy = SupersededDegraded
	ctx := context.Background()

	req := standardRequest()
	req.PayMethod = PayMethodAlipay
	old := seedSupersedeOldOrder(t, env, req)

	env.gw.CloseOrderErr = errors.New("gateway 5xx")

	result, err := env.engine.Create(ctx, req)
	mustNotErr(t, err, "Create should succeed in Degraded mode despite gateway failure")
	if result.Order == nil {
		t.Fatal("expected new order")
	}
	if result.Order.OrderNo() == old.orderNo {
		t.Fatal("expected new order, got old")
	}

	// 旧单已通过本地 CAS Close 推到 Closed 状态
	mustEqual(t, old.status, StatusClosed, "old order CAS-closed locally")
	// 新单已创建成功
	mustEqual(t, env.store.CreateCalls, 1, "new order created")
	mustEqual(t, env.gw.UnifiedOrderCalls, 1, "new UnifiedOrder called")

	// OnSuperseded + OnClosed 仍然会触发（CAS Close 走本地路径成功）
	mustLen(t, env.OnSupersededCalls, 1, "OnSuperseded fired")
	mustLen(t, env.OnClosedCalls, 1, "OnClosed fired")
	mustEqual(t, env.OnClosedCalls[0].Reason, ClosedReasonSuperseded, "OnClosed reason")
}

// P1#3: Degraded + 网关失败 + CAS 成功 → 必须 emit EventSupersededGatewayCloseFailed 并触发 hook。
func TestCreate_SupersededGatewayFailedTriggersHookOnDegraded(t *testing.T) {
	env := newTestEnv(t)
	env.engine.closeSupersededPolicy = SupersededDegraded
	ctx := context.Background()

	req := standardRequest()
	req.PayMethod = PayMethodAlipay
	old := seedSupersedeOldOrder(t, env, req)

	gwErr := errors.New("gateway 5xx persistent")
	env.gw.CloseOrderErr = gwErr

	_, err := env.engine.Create(ctx, req)
	mustNotErr(t, err, "Create should succeed in Degraded mode")
	mustEqual(t, old.status, StatusClosed, "old order locally Closed")

	// Observer 必须收到 EventSupersededGatewayCloseFailed
	mustEqual(t, env.observer.countByKind(EventSupersededGatewayCloseFailed), 1,
		"EventSupersededGatewayCloseFailed emitted exactly once")
	ev, _ := env.observer.firstByKind(EventSupersededGatewayCloseFailed)
	if got := ev.Attrs["kind"]; got != string(AnomalySupersededGatewayCloseFailed) {
		t.Fatalf("anomaly kind = %v, want %s", got, AnomalySupersededGatewayCloseFailed)
	}
	if got := ev.Attrs["reason"].(string); got == "" {
		t.Fatal("reason attribute missing")
	}

	// hook 必须被调用，且参数完整
	mustLen(t, env.OnSupersededGatewayFailedCalls, 1, "hook fired once")
	call := env.OnSupersededGatewayFailedCalls[0]
	mustEqual(t, call.OldOrderNo, old.orderNo, "hook receives old order no")
	mustEqual(t, call.NewProductID, req.Product.ID, "hook receives new product id")
	if !errors.Is(call.GatewayErr, gwErr) && !errContains(call.GatewayErr, "gateway 5xx") {
		t.Fatalf("hook gatewayErr = %v, want wrap of %v", call.GatewayErr, gwErr)
	}
}

// P1#3: Strict 模式不触发 hook（早期 return，没机会执行到 hook 代码）。
func TestCreate_SupersededGatewayFailedNotTriggeredOnStrict(t *testing.T) {
	env := newTestEnv(t)
	// 默认 SupersededStrict，不显式设置
	ctx := context.Background()

	req := standardRequest()
	req.PayMethod = PayMethodAlipay
	seedSupersedeOldOrder(t, env, req)

	env.gw.CloseOrderErr = errors.New("gateway 5xx")

	_, err := env.engine.Create(ctx, req)
	if err == nil {
		t.Fatal("expected Strict error")
	}

	// Strict 模式下 hook 与 Event 都不应该触发
	mustEqual(t, env.observer.countByKind(EventSupersededGatewayCloseFailed), 0,
		"no event in Strict mode")
	mustLen(t, env.OnSupersededGatewayFailedCalls, 0, "no hook in Strict mode")
}

// P1#3: Degraded + 网关失败 + CAS race（旧单已被并发推到 Paid）→ 不触发 hook。
// 设计意图：hook 语义是"订单确实被本次操作推到 Closed"，CAS 抢跑失败时订单
// 已是 Paid，hook 失去意义。
func TestCreate_SupersededGatewayFailedNotTriggeredOnCASRaceLost(t *testing.T) {
	env := newTestEnv(t)
	env.engine.closeSupersededPolicy = SupersededDegraded
	ctx := context.Background()

	req := standardRequest()
	req.PayMethod = PayMethodAlipay
	old := seedSupersedeOldOrder(t, env, req)

	// 网关失败 + 模拟 CAS race：CASClose 第一次返回 affected=0 且把旧单改成 Paid
	env.gw.CloseOrderErr = errors.New("gateway 5xx")
	env.store.CASCloseLosesToPaidOnce = true

	result, err := env.engine.Create(ctx, req)
	mustNotErr(t, err, "Create should succeed (return current Paid order)")
	if result == nil || !result.Reused {
		t.Fatal("expected Reused=true with the Paid order")
	}
	mustEqual(t, old.status, StatusPaid, "old order won race -> Paid")

	// hook 与 Event 都不应该触发（CAS 没成功推 Closed）
	mustEqual(t, env.observer.countByKind(EventSupersededGatewayCloseFailed), 0,
		"no event when CAS lost race")
	mustLen(t, env.OnSupersededGatewayFailedCalls, 0, "no hook when CAS lost race")
}

// delay-queue-cleanup-consistency：closeSuperseded 路径下 delayQueue.Remove 失败
// 必须 emit AnomalyDelayQueueCleanupFailed（之前是 `_ = Remove(...)` 静默吞错）。
func TestCreate_SupersededDelayQueueRemoveFailureEmitsAnomaly(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	req := standardRequest()
	req.PayMethod = PayMethodAlipay
	seedSupersedeOldOrder(t, env, req)

	// 注入 delay queue Remove 失败
	env.dq.ErrOnRemove = errors.New("redis cluster unreachable")

	result, err := env.engine.Create(ctx, req)
	mustNotErr(t, err, "Create should succeed despite queue Remove failure")
	if result == nil || result.Order == nil {
		t.Fatal("expected new order")
	}

	// 必须 emit AnomalyDelayQueueCleanupFailed（通过 OnAnomaly hook 验证最易断言）
	found := false
	for _, c := range env.OnAnomalyCalls {
		if c.Kind == AnomalyDelayQueueCleanupFailed {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected AnomalyDelayQueueCleanupFailed in OnAnomalyCalls, got %v", env.OnAnomalyCalls)
	}
}
