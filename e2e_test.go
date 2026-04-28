package orderflow

import (
	"context"
	"testing"
	"time"
)

// =============================================================================
// TestE2E_FullOrderLifecycle：订单全流程闭环验证
// =============================================================================
//
// 这是 orderflow 的"金标"测试——用一个 narrative 连接 Create 下单 → 支付回调 →
// 履约 → 查询的全部环节，每一步都交叉验证所有可观测信号：
//
//   Store 状态 + Cache 值 + Stream 事件 + DelayQueue 成员 + 日志条数 +
//   Observer Event / Duration + 各钩子调用 + Bill 写入 + Anomaly 计数
//
// 这个测试的价值是"闭环证明"：任何环节的代码改动都可能破坏端到端不变量，
// E2E 测试作为最后一道门槛，防止局部优化造成全局破坏。

func TestE2E_FullOrderLifecycle(t *testing.T) {
	env, obs := newTestEnvWithObserver(t)
	ctx := context.Background()

	// ============================================================
	// 阶段 1：下单 Create
	// ============================================================
	req := standardRequest() // userID=1001, productID=2001, PayMethod=wechat, Price=9900

	createResult, err := env.engine.Create(ctx, req)
	mustNotErr(t, err, "Phase 1: Create")

	// 断言 Create 返回值
	orderNo := createResult.Order.OrderNo()
	orderToken := createResult.Order.OrderToken()
	if orderNo == "" || orderToken == "" {
		t.Fatalf("Phase 1: order no/token empty: %q, %q", orderNo, orderToken)
	}
	mustEqual(t, createResult.Reused, false, "Phase 1: not reused")
	mustEqual(t, createResult.PaymentParams, "mock_pay_params", "Phase 1: payment params")
	mustEqual(t, createResult.Order.Status(), StatusPending, "Phase 1: order Status")
	mustEqual(t, createResult.Order.UserID(), req.UserID, "Phase 1: order UserID")
	mustEqual(t, createResult.Order.PayAmount(), req.Product.Price, "Phase 1: order PayAmount")

	// 断言 Store：订单落库，状态 Pending
	mustEqual(t, env.store.CreateCalls, 1, "Phase 1: Store.Create invoked once")
	persisted := env.store.byNo[orderNo]
	if persisted == nil {
		t.Fatal("Phase 1: order not in store.byNo")
	}
	mustEqual(t, persisted.status, StatusPending, "Phase 1: persisted Status")

	// 断言 Store：已写"created" 日志
	mustEqual(t, env.store.AppendLogCalls, 1, "Phase 1: AppendLog invoked")
	mustEqual(t, env.store.logs[0].ToStatus, StatusPending, "Phase 1: log ToStatus")
	mustEqual(t, env.store.logs[0].Remark, "created", "Phase 1: log Remark")

	// 断言 DelayQueue：订单在延时队列里等待超时关闭
	mustEqual(t, env.dq.EnqueueCalls, 1, "Phase 1: DelayQueue.Enqueue")
	if _, enqueued := env.dq.enqueued[orderNo]; !enqueued {
		t.Fatalf("Phase 1: order %s not in delay queue", orderNo)
	}

	// 断言 Cache：Pending 状态已缓存
	mustEqual(t, env.cache.SetCalls, 1, "Phase 1: cache.Set")
	cached, hit, err := env.cache.Get(ctx, orderToken)
	mustNotErr(t, err, "Phase 1: cache.Get")
	if !hit {
		t.Fatal("Phase 1: cache miss after Create")
	}
	mustEqual(t, cached.Status, StatusPending, "Phase 1: cache status")
	mustEqual(t, cached.UserID, req.UserID, "Phase 1: cache userID")

	// 断言 Gateway：UnifiedOrder 被调用（为客户端拉起支付）
	mustEqual(t, env.gw.UnifiedOrderCalls, 1, "Phase 1: Gateway.UnifiedOrder")

	// 断言 Hook：OnCreated 触发，其他钩子未触发
	mustLen(t, env.OnCreatedCalls, 1, "Phase 1: OnCreated")
	mustLen(t, env.OnPaidCalls, 0, "Phase 1: no OnPaid yet")
	mustLen(t, env.OnClosedCalls, 0, "Phase 1: no OnClosed")
	mustLen(t, env.OnAnomalyCalls, 0, "Phase 1: no anomaly")

	// 断言 Observer：Created event + Create duration（成功）
	mustLen(t, obs.byKind(EventOrderCreated), 1, "Phase 1: observer Created")
	mustLen(t, obs.durationsByOp(OpCreate), 1, "Phase 1: observer Create duration")
	if d := obs.durationsByOp(OpCreate)[0]; d.Error != nil {
		t.Errorf("Phase 1: observer Create error = %v, want nil", d.Error)
	}

	// ============================================================
	// 阶段 2：客户端轮询——缓存命中 Pending
	// ============================================================
	poll, err := env.engine.PollStatus(ctx, orderToken, req.UserID)
	mustNotErr(t, err, "Phase 2: PollStatus")
	mustEqual(t, poll.Status, StatusPending, "Phase 2: poll returns Pending")

	// 应该是缓存命中，不回源 DB
	// 验证方式：缓存 Get 次数 +1，Store.GetByToken 次数 0
	mustEqual(t, env.cache.GetCalls, 2, "Phase 2: cache Get (1 from test setup + 1 from PollStatus)")

	// ============================================================
	// 阶段 3：支付网关回调 HandleNotify
	// ============================================================
	env.gw.NotifyResult = NotifyResult{
		OutTradeNo:    orderNo,
		TransactionID: "TXN-E2E-001",
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   req.Product.Price,
		PaidAt:        time.Now(),
		Channel:       "wechat",
	}

	mustNotErr(t, env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()), "Phase 3: HandleNotify")

	// 断言 Store：订单推进到 Delivered，账单写入
	mustEqual(t, env.store.CASConfirmPaidCalls, 1, "Phase 3: CAS confirm paid")
	mustEqual(t, env.store.FinalizeCalls, 1, "Phase 3: Finalize")
	mustLen(t, env.store.bills, 1, "Phase 3: 1 bill written")
	mustEqual(t, env.store.bills[0].TradeNo, "TXN-E2E-001", "Phase 3: bill TradeNo")
	mustEqual(t, env.store.bills[0].PayAmount, req.Product.Price, "Phase 3: bill PayAmount")
	mustEqual(t, env.store.byNo[orderNo].status, StatusDelivered, "Phase 3: order Delivered")

	// 断言 DelayQueue：任务已从队列移除（支付成功无需再超时关闭）
	if env.dq.RemoveCalls < 1 {
		t.Errorf("Phase 3: DelayQueue.Remove not called")
	}
	if _, stillEnqueued := env.dq.enqueued[orderNo]; stillEnqueued {
		t.Errorf("Phase 3: order %s still in delay queue after paid", orderNo)
	}

	// 断言 Stream：至少 2 次 publish（Paid 和 Delivered）
	var paidPub, deliveredPub int
	for _, p := range env.stream.Published {
		if p.OrderToken != orderToken {
			continue
		}
		switch p.Status {
		case StatusPaid:
			paidPub++
		case StatusDelivered:
			deliveredPub++
		}
	}
	if paidPub < 1 {
		t.Errorf("Phase 3: missing Paid stream publish")
	}
	if deliveredPub < 1 {
		t.Errorf("Phase 3: missing Delivered stream publish")
	}

	// 断言 Cache：最终状态是 Delivered
	cached, hit, err = env.cache.Get(ctx, orderToken)
	mustNotErr(t, err, "Phase 3: cache.Get")
	if !hit {
		t.Fatal("Phase 3: cache miss after delivery")
	}
	mustEqual(t, cached.Status, StatusDelivered, "Phase 3: cache is Delivered")

	// 断言 Hook：OnPaid + OnDelivered 各一次
	mustLen(t, env.OnPaidCalls, 1, "Phase 3: OnPaid")
	mustEqual(t, env.OnPaidCalls[0].OrderNo, orderNo, "Phase 3: OnPaid orderNo")
	mustEqual(t, env.OnPaidCalls[0].TradeNo, "TXN-E2E-001", "Phase 3: OnPaid tradeNo")
	mustLen(t, env.OnDeliveredCalls, 1, "Phase 3: OnDelivered")

	// 断言 Observer：Paid + Delivered 事件 + HandleNotify duration 成功
	mustLen(t, obs.byKind(EventOrderPaid), 1, "Phase 3: observer Paid")
	mustLen(t, obs.byKind(EventOrderDelivered), 1, "Phase 3: observer Delivered")
	mustLen(t, obs.durationsByOp(OpHandleNotify), 1, "Phase 3: observer HandleNotify duration")
	if d := obs.durationsByOp(OpHandleNotify)[0]; d.Error != nil {
		t.Errorf("Phase 3: HandleNotify error = %v", d.Error)
	}

	// 零异常（整条路径没有任何 Anomaly）
	mustLen(t, env.OnAnomalyCalls, 0, "Phase 3: no anomaly in full lifecycle")
	mustLen(t, obs.byKind(EventAnomaly), 0, "Phase 3: observer no anomaly")

	// ============================================================
	// 阶段 4：客户端再次轮询——应读到 Delivered
	// ============================================================
	poll, err = env.engine.PollStatus(ctx, orderToken, req.UserID)
	mustNotErr(t, err, "Phase 4: PollStatus")
	mustEqual(t, poll.Status, StatusDelivered, "Phase 4: poll returns Delivered")

	// 跨用户查询应被拒
	_, err = env.engine.PollStatus(ctx, orderToken, 99999)
	if err != ErrOrderForbidden {
		t.Errorf("Phase 4: foreign user PollStatus = %v, want ErrOrderForbidden", err)
	}

	// ============================================================
	// 阶段 5：查询 Timeline——应看到完整流水
	// ============================================================
	tl, err := env.engine.Timeline(ctx, orderToken, req.UserID)
	mustNotErr(t, err, "Phase 5: Timeline")
	mustEqual(t, tl.OrderNo, orderNo, "Phase 5: Timeline.OrderNo")
	mustEqual(t, tl.Status, StatusDelivered, "Phase 5: Timeline.Status")

	// Timeline 至少有 created + paid 两条；Finalize 没单独写日志，以当前实现行为为准
	if len(tl.Entries) < 2 {
		t.Errorf("Phase 5: Timeline too short: %d entries", len(tl.Entries))
	}

	// 跨用户查询 Timeline 也应被拒
	_, err = env.engine.Timeline(ctx, orderToken, 99999)
	if err != ErrOrderForbidden {
		t.Errorf("Phase 5: foreign user Timeline = %v, want ErrOrderForbidden", err)
	}

	// ============================================================
	// 阶段 6：重入 HandleNotify——幂等保证（不重复 finalize）
	// ============================================================
	finalizeCallsBefore := env.store.FinalizeCalls
	billsBefore := len(env.store.bills)
	onPaidCallsBefore := len(env.OnPaidCalls)

	mustNotErr(t, env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()), "Phase 6: duplicate HandleNotify")

	// Delivered 状态下收到 notify 直接 skip
	mustEqual(t, env.store.FinalizeCalls, finalizeCallsBefore, "Phase 6: no new Finalize (idempotent)")
	mustEqual(t, len(env.store.bills), billsBefore, "Phase 6: no new bill")
	mustEqual(t, len(env.OnPaidCalls), onPaidCallsBefore, "Phase 6: no OnPaid re-entry")

	// ============================================================
	// 最终总结：所有环节都闭环
	// ============================================================
	t.Logf("E2E summary: orderNo=%s orderToken=%s", orderNo, orderToken)
	t.Logf("  Store: create=%d finalize=%d bills=%d logs=%d",
		env.store.CreateCalls, env.store.FinalizeCalls,
		len(env.store.bills), len(env.store.logs))
	t.Logf("  Cache: set=%d get=%d", env.cache.SetCalls, env.cache.GetCalls)
	t.Logf("  Stream: published=%d", len(env.stream.Published))
	t.Logf("  DelayQueue: enqueue=%d remove=%d", env.dq.EnqueueCalls, env.dq.RemoveCalls)
	t.Logf("  Gateway: unified=%d parse=%d", env.gw.UnifiedOrderCalls, env.gw.ParseNotifyCalls)
	t.Logf("  Hooks: created=%d paid=%d delivered=%d closed=%d anomaly=%d",
		len(env.OnCreatedCalls), len(env.OnPaidCalls), len(env.OnDeliveredCalls),
		len(env.OnClosedCalls), len(env.OnAnomalyCalls))
	t.Logf("  Observer: events=%d durations=%d", len(obs.Events), len(obs.Durations))
}

// TestE2E_TimeoutCloseCycle：超时关闭分支的闭环
//
// 下单 → 不支付 → 过期 → Close 被触发 → Closed 状态 → 客户端 Poll 看到 Closed
func TestE2E_TimeoutCloseCycle(t *testing.T) {
	env, obs := newTestEnvWithObserver(t)
	ctx := context.Background()

	// 下单
	req := standardRequest()
	result, err := env.engine.Create(ctx, req)
	mustNotErr(t, err, "Create")
	orderNo := result.Order.OrderNo()
	orderToken := result.Order.OrderToken()

	// 快进到过期：直接改 store 里的 expireAt
	env.store.byNo[orderNo].expireAt = time.Now().Add(-time.Minute)

	// 模拟 worker 触发 Close
	mustNotErr(t, env.engine.Close(ctx, orderNo), "Close")

	// 订单 Closed
	mustEqual(t, env.store.byNo[orderNo].status, StatusClosed, "order Closed")

	// 客户端 Poll 读到 Closed
	poll, err := env.engine.PollStatus(ctx, orderToken, req.UserID)
	mustNotErr(t, err, "PollStatus")
	mustEqual(t, poll.Status, StatusClosed, "poll returns Closed")

	// Observer：Closed event 带 reason=timeout + Close duration
	closed := obs.byKind(EventOrderClosed)
	mustLen(t, closed, 1, "Closed events")
	if reason, _ := closed[0].Attrs["reason"].(string); reason != string(ClosedReasonTimeout) {
		t.Errorf("closed reason = %v", closed[0].Attrs["reason"])
	}
	mustLen(t, obs.durationsByOp(OpClose), 1, "Close duration")

	// 零异常
	mustLen(t, env.OnAnomalyCalls, 0, "no anomaly")
}

// TestE2E_CloseThenPaidRecovery：已关闭订单被网关确认已支付的恢复路径
//
// 下单 → 超时关闭 → 延迟到达的支付回调 → 网关 Query 确认已付 → CASReopenPaid → Delivered
func TestE2E_CloseThenPaidRecovery(t *testing.T) {
	env, obs := newTestEnvWithObserver(t)
	ctx := context.Background()

	// 先造一个已 Closed 的订单
	env.store.seed(&testOrder{
		orderNo:       "RECOV-1",
		orderToken:    "T-RECOV",
		userID:        1001,
		status:        StatusClosed,
		productID:     2001,
		productTitle:  "VIP",
		originalPrice: 9900,
		payAmount:     9900,
		payMethod:     PayMethodWechat,
		expireAt:      time.Now().Add(-time.Hour),
	})

	// 支付回调到达
	env.gw.NotifyResult = makeNotify("RECOV-1", 9900, "TXN-RECOV")
	// 网关 Query 返回：确实已支付
	env.gw.QueryResp = QueryResult{
		OutTradeNo:    "RECOV-1",
		TransactionID: "TXN-RECOV",
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   9900,
		PaidAt:        time.Now(),
		Channel:       "wechat",
	}

	mustNotErr(t, env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 订单恢复并推进到 Delivered
	mustEqual(t, env.store.byNo["RECOV-1"].status, StatusDelivered, "final status")
	mustEqual(t, env.store.CASReopenPaidCalls, 1, "CASReopenPaid called")
	mustEqual(t, env.store.FinalizeCalls, 1, "Finalize called")

	// 钩子：OnReopened + OnPaid + OnDelivered
	mustLen(t, env.OnReopenedCalls, 1, "OnReopened")
	mustLen(t, env.OnPaidCalls, 1, "OnPaid")
	mustLen(t, env.OnDeliveredCalls, 1, "OnDelivered")

	// Observer：Reopened + Paid + Delivered 三个 event
	mustLen(t, obs.byKind(EventOrderReopened), 1, "observer Reopened")
	mustLen(t, obs.byKind(EventOrderPaid), 1, "observer Paid")
	mustLen(t, obs.byKind(EventOrderDelivered), 1, "observer Delivered")
}
