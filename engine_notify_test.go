package orderflow

import (
	"context"
	"testing"
	"time"
)

// HandleNotify 的交叉验证覆盖：
//   - 状态分派：Pending / Paid / Closed / Delivered / 未知 状态的正确走向
//   - 幂等：重复通知不重复履约
//   - 异常：金额不一致 / 交易号不一致 / 已关闭回调 Paid 走 reopen 分支
//   - 副作用：OnPaid 钩子、Finalize、Delivered 推送
//
// 每个场景验证：订单最终状态、钩子调用次数、OnAnomaly 触发种类、cache/stream 事件。

// seedPendingOrder 构造一个典型的 Pending 订单并 seed 到 store。
func seedPendingOrder(env *testEnv, orderNo string) *testOrder {
	o := &testOrder{
		orderNo:       orderNo,
		orderToken:    "TOK-" + orderNo,
		userID:        1001,
		status:        StatusPending,
		productID:     2001,
		productTitle:  "VIP",
		originalPrice: 9900,
		payAmount:     9900,
		payMethod:     PayMethodWechat,
		expireAt:      time.Now().Add(time.Hour),
	}
	env.store.seed(o)
	return o
}

// makeNotify 构造与订单匹配的 NotifyResult
func makeNotify(orderNo string, amount int64, tradeNo string) NotifyResult {
	return NotifyResult{
		OutTradeNo:    orderNo,
		TransactionID: tradeNo,
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   amount,
		PaidAt:        time.Now(),
		Channel:       "wechat",
	}
}

func TestHandleNotify_PendingToPaidHappyPath(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	seedPendingOrder(env, "NO-1")
	env.gw.NotifyResult = makeNotify("NO-1", 9900, "TXN-1")

	mustNotErr(t, env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	o := env.store.byNo["NO-1"]
	// 推进到最终态 Delivered（OnPaid + Finalize 完成）
	mustEqual(t, o.status, StatusDelivered, "final status")
	mustEqual(t, o.tradeNo, "TXN-1", "tradeNo written")

	// CAS + Finalize
	mustEqual(t, env.store.CASConfirmPaidCalls, 1, "CASConfirmPaid")
	mustEqual(t, env.store.FinalizeCalls, 1, "Finalize")
	mustLen(t, env.store.bills, 1, "bill written")

	// 延时队列被清理
	mustEqual(t, env.dq.RemoveCalls, 1, "delay queue Remove")

	// Stream publish 序列：Paid -> Delivered（各一次）
	if len(env.stream.Published) != 2 {
		t.Fatalf("expected 2 stream events, got %d: %+v", len(env.stream.Published), env.stream.Published)
	}
	mustEqual(t, env.stream.Published[0].Status, StatusPaid, "1st publish")
	mustEqual(t, env.stream.Published[1].Status, StatusDelivered, "2nd publish")

	// 钩子
	mustLen(t, env.OnPaidCalls, 1, "OnPaid")
	mustEqual(t, env.OnPaidCalls[0].TradeNo, "TXN-1", "OnPaid tradeNo")
	mustLen(t, env.OnDeliveredCalls, 1, "OnDelivered")
	mustLen(t, env.OnAnomalyCalls, 0, "no anomaly")
}

func TestHandleNotify_IgnoresNonPaidTradeStatus(t *testing.T) {
	env := newTestEnv(t)
	seedPendingOrder(env, "NO-X")
	env.gw.NotifyResult = NotifyResult{
		OutTradeNo:  "NO-X",
		TradeStatus: TradeStatusUnpaid,
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 订单不应被推进
	mustEqual(t, env.store.byNo["NO-X"].status, StatusPending, "status unchanged")
	mustEqual(t, env.store.CASConfirmPaidCalls, 0, "no CAS")
	mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
}

func TestHandleNotify_AmountMismatchOnPending(t *testing.T) {
	env := newTestEnv(t)
	seedPendingOrder(env, "NO-2")
	// 通知金额与订单不一致
	env.gw.NotifyResult = makeNotify("NO-2", 1, "TXN-X")

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 订单保持 Pending，OnAnomaly 上报
	mustEqual(t, env.store.byNo["NO-2"].status, StatusPending, "status unchanged")
	mustEqual(t, env.store.CASConfirmPaidCalls, 0, "no CAS")
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyAmountMismatch, "anomaly kind")
}

func TestHandleNotify_OrderNotFound(t *testing.T) {
	env := newTestEnv(t)
	env.gw.NotifyResult = makeNotify("GHOST", 9900, "TXN-G")

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	mustEqual(t, env.store.CASConfirmPaidCalls, 0, "no CAS")
	mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
	// 没有订单，OnAnomaly 也不会触发（Engine 只 log 一下）
	mustLen(t, env.OnAnomalyCalls, 0, "no anomaly without order")
}

func TestHandleNotify_IdempotentOnDelivered(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:    "NO-D",
		orderToken: "T-D",
		userID:     1001,
		status:     StatusDelivered,
		payAmount:  9900,
		payMethod:  PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-D", 9900, "TXN-D")

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	mustLen(t, env.OnPaidCalls, 0, "no OnPaid on delivered retry")
	mustEqual(t, env.store.FinalizeCalls, 0, "no Finalize")
	mustEqual(t, env.store.CASConfirmPaidCalls, 0, "no CAS")
}

func TestHandleNotify_PaidRetryRefinalizes(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// 已 Paid 但 Finalize 失败过的订单：再次收到通知时应重新走 Finalize
	paidAt := time.Now().Add(-time.Minute)
	env.store.seed(&testOrder{
		orderNo:       "NO-R",
		orderToken:    "T-R",
		userID:        1001,
		status:        StatusPaid,
		payAmount:     9900,
		originalPrice: 9900,
		productTitle:  "VIP",
		payMethod:     PayMethodWechat,
		tradeNo:       "TXN-R",
		paidAt:        &paidAt,
	})
	env.gw.NotifyResult = makeNotify("NO-R", 9900, "TXN-R")

	mustNotErr(t, env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// Finalize 和 OnPaid 都触发
	mustLen(t, env.OnPaidCalls, 1, "OnPaid")
	mustEqual(t, env.store.FinalizeCalls, 1, "Finalize")
	mustEqual(t, env.store.byNo["NO-R"].status, StatusDelivered, "final status")
	// 但 CASConfirmPaid 不应再跑（已 Paid）
	mustEqual(t, env.store.CASConfirmPaidCalls, 0, "no CAS on paid retry")
}

func TestHandleNotify_PaidRetryWithTradeNoMismatchSkipped(t *testing.T) {
	env := newTestEnv(t)
	paidAt := time.Now()
	env.store.seed(&testOrder{
		orderNo:   "NO-TN",
		status:    StatusPaid,
		payAmount: 9900,
		payMethod: PayMethodWechat,
		tradeNo:   "TXN-ORIGINAL",
		paidAt:    &paidAt,
	})
	// 通知里的 trade_no 与已存储的不同
	env.gw.NotifyResult = makeNotify("NO-TN", 9900, "TXN-DIFFERENT")

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 不 re-finalize，只报异常
	mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
	mustEqual(t, env.store.FinalizeCalls, 0, "no Finalize")
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyTradeNoMismatch, "anomaly kind")
}

func TestHandleNotify_ClosedOrderReopenedByGatewayConfirm(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// 订单已被本地关闭（例如超时关单刚跑完），但支付回调后来了
	env.store.seed(&testOrder{
		orderNo:       "NO-RE",
		orderToken:    "T-RE",
		userID:        1001,
		status:        StatusClosed,
		payAmount:     9900,
		originalPrice: 9900,
		productTitle:  "VIP",
		payMethod:     PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-RE", 9900, "TXN-RE")
	// 网关查询确认：的确已支付
	env.gw.QueryResp = QueryResult{
		OutTradeNo:    "NO-RE",
		TransactionID: "TXN-RE",
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   9900,
		PaidAt:        time.Now(),
		Channel:       "wechat",
	}

	mustNotErr(t, env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 状态恢复并推进：Closed -> Paid -> Delivered
	mustEqual(t, env.store.byNo["NO-RE"].status, StatusDelivered, "final status")
	mustEqual(t, env.store.CASReopenPaidCalls, 1, "CASReopenPaid")
	mustEqual(t, env.store.FinalizeCalls, 1, "Finalize")

	// 钩子：OnReopened + OnPaid + OnDelivered
	mustLen(t, env.OnReopenedCalls, 1, "OnReopened")
	mustLen(t, env.OnPaidCalls, 1, "OnPaid")
	mustLen(t, env.OnDeliveredCalls, 1, "OnDelivered")
}

// 补：Query 3 次重试全失败 → OnAnomaly GatewayQueryFailed，订单保持 Closed
func TestHandleNotify_ClosedButGatewayQueryKeepsFailing(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:    "NO-QERR",
		orderToken: "T-QERR",
		userID:     1001,
		status:     StatusClosed,
		payAmount:  9900,
		payMethod:  PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-QERR", 9900, "TXN")
	env.gw.QueryErr = errTestQueryDown // 让 Query 持续失败

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 订单保持 Closed，无 Reopen / Finalize
	mustEqual(t, env.store.byNo["NO-QERR"].status, StatusClosed, "status unchanged")
	mustEqual(t, env.store.CASReopenPaidCalls, 0, "no reopen")
	mustEqual(t, env.store.FinalizeCalls, 0, "no finalize")
	// Query 被重试 3 次
	mustEqual(t, env.gw.QueryOrderCalls, 3, "Query retried 3 times")
	// OnAnomaly 上报 GatewayQueryFailed
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyGatewayQueryFailed, "anomaly kind")
	mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
	mustLen(t, env.OnReopenedCalls, 0, "no OnReopened")
}

// 补：Query 成功但金额与订单不匹配 → OnAnomaly AmountMismatch，订单保持 Closed
func TestHandleNotify_ClosedQueryAmountMismatch(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:    "NO-AM",
		orderToken: "T-AM",
		userID:     1001,
		status:     StatusClosed,
		payAmount:  9900,
		payMethod:  PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-AM", 9900, "TXN")
	// 网关 Query 返回 Paid，但金额异常（中间人攻击或网关 bug 场景）
	env.gw.QueryResp = QueryResult{
		OutTradeNo:  "NO-AM",
		TradeStatus: TradeStatusPaid,
		TotalAmount: 1, // 假金额
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 订单保持 Closed，不允许 Reopen
	mustEqual(t, env.store.byNo["NO-AM"].status, StatusClosed, "status unchanged")
	mustEqual(t, env.store.CASReopenPaidCalls, 0, "no reopen")
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyAmountMismatch, "anomaly kind")
}

// 补：CASReopenPaid 返回 error → OnAnomaly UnexpectedStatus，订单保持 Closed
func TestHandleNotify_ClosedCASReopenError(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:    "NO-CE",
		orderToken: "T-CE",
		userID:     1001,
		status:     StatusClosed,
		payAmount:  9900,
		payMethod:  PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-CE", 9900, "TXN")
	env.gw.QueryResp = QueryResult{
		OutTradeNo:    "NO-CE",
		TransactionID: "TXN",
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   9900,
	}
	env.store.ErrOnCAS = errTestCASDown // 让 CAS 直接报错

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// CAS 被尝试了
	mustEqual(t, env.store.CASReopenPaidCalls, 1, "CASReopenPaid attempted")
	// OnAnomaly 记录为 UnexpectedStatus（engine 代码里就这样命名）
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyUnexpectedStatus, "anomaly kind")
	// Finalize 不应调用
	mustEqual(t, env.store.FinalizeCalls, 0, "no finalize")
}

// 补：CASReopenPaid 返回 0（affected=0，并发抢跑已被其他 worker 推进）→ 静默 return，无 anomaly
// 用 fakeStore.CASReopenMissOnce 注入：让第一次 CAS 返回 0 不改状态，模拟真实并发 miss。
func TestHandleNotify_ClosedReopenMissed(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:    "NO-MISS",
		orderToken: "T-MISS",
		userID:     1001,
		status:     StatusClosed,
		payAmount:  9900,
		payMethod:  PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-MISS", 9900, "TXN")
	env.gw.QueryResp = QueryResult{
		OutTradeNo:    "NO-MISS",
		TransactionID: "TXN",
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   9900,
	}
	// 注入 CAS miss：第一次 CASReopenPaid 返回 0（模拟并发抢跑）
	env.store.CASReopenMissOnce = true

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// CAS 被尝试了一次
	mustEqual(t, env.store.CASReopenPaidCalls, 1, "CASReopenPaid attempted once")
	// 订单保持 Closed（CAS 没成功）
	mustEqual(t, env.store.byNo["NO-MISS"].status, StatusClosed, "status unchanged (CAS missed)")
	// 不应触发 Finalize / OnPaid / OnReopened / OnAnomaly
	mustEqual(t, env.store.FinalizeCalls, 0, "no finalize")
	mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
	mustLen(t, env.OnReopenedCalls, 0, "no OnReopened")
	mustLen(t, env.OnAnomalyCalls, 0, "no anomaly (CAS miss is expected, not an error)")
}

// 超时侧补：网关第一次关闭返回瞬时错误、第二次成功——CloseFallback 兜底重试链路
// 流程：超时订单 A → 首次 Close 因网关错误失败 → 订单仍 Pending → fallback 下一轮 Close → 成功。
func TestCloseFallback_RetriesAfterTransientGatewayFailure(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	env.store.seed(&testOrder{
		orderNo:    "RETRY-1",
		orderToken: "T-RETRY",
		userID:     1001,
		status:     StatusPending,
		payMethod:  PayMethodWechat,
		expireAt:   time.Now().Add(-time.Minute),
	})

	// 第一次网关失败
	env.gw.CloseOrderErr = errTestGatewayTimeout
	err := env.engine.Close(ctx, "RETRY-1")
	if err == nil {
		t.Fatal("first Close should fail")
	}
	mustEqual(t, env.store.byNo["RETRY-1"].status, StatusPending, "still Pending after gateway failure")

	// 模拟下一轮兜底扫描：FindExpiredPending 应该能再次找到这个订单
	expired, err := env.engine.FindExpiredPending(ctx, 100)
	mustNotErr(t, err, "FindExpiredPending")
	mustLen(t, expired, 1, "order still in expired list")
	mustEqual(t, expired[0], "RETRY-1", "orderNo")

	// 网关恢复，第二次 Close 成功
	env.gw.CloseOrderErr = nil
	mustNotErr(t, env.engine.Close(ctx, "RETRY-1"), "retry Close succeeds")
	mustEqual(t, env.store.byNo["RETRY-1"].status, StatusClosed, "finally Closed")
	mustLen(t, env.OnClosedCalls, 1, "OnClosed fired once")
}

var (
	errTestQueryDown      = &testHookError{msg: "gateway query service down"}
	errTestCASDown        = &testHookError{msg: "db deadlock on CAS"}
	errTestGatewayTimeout = &testHookError{msg: "gateway: connect timeout"}
)

func TestHandleNotify_ClosedButGatewayReportsNotPaid(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:   "NO-CL",
		status:    StatusClosed,
		payAmount: 9900,
		payMethod: PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-CL", 9900, "TXN")
	// 网关查询：其实未支付（矛盾情况）
	env.gw.QueryResp = QueryResult{
		OutTradeNo:  "NO-CL",
		TradeStatus: TradeStatusUnpaid,
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 订单保持 Closed，不应 reopen
	mustEqual(t, env.store.byNo["NO-CL"].status, StatusClosed, "status unchanged")
	mustEqual(t, env.store.CASReopenPaidCalls, 0, "no reopen")
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyPaidOnClosed, "anomaly kind")
}

func TestHandleNotify_CancelledPaidConfirmedDoesNotReopenOrFinalize(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:       "NO-CANCELLED-PAID",
		orderToken:    "T-CANCELLED-PAID",
		userID:        1001,
		status:        StatusCancelled,
		payAmount:     9900,
		originalPrice: 9900,
		productTitle:  "VIP",
		payMethod:     PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-CANCELLED-PAID", 9900, "TXN-CANCELLED-PAID")
	env.gw.QueryResp = QueryResult{
		OutTradeNo:    "NO-CANCELLED-PAID",
		TransactionID: "TXN-CANCELLED-PAID",
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   9900,
		PaidAt:        time.Now(),
		Channel:       "wechat",
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	mustEqual(t, env.gw.QueryOrderCalls, 1, "QueryOrder called")
	mustEqual(t, env.store.byNo["NO-CANCELLED-PAID"].status, StatusCancelled, "status unchanged")
	mustEqual(t, env.store.CASConfirmPaidCalls, 0, "no confirm")
	mustEqual(t, env.store.CASReopenPaidCalls, 0, "no reopen")
	mustEqual(t, env.store.FinalizeCalls, 0, "no finalize")
	mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
	mustLen(t, env.OnReopenedCalls, 0, "no OnReopened")
	mustLen(t, env.OnDeliveredCalls, 0, "no OnDelivered")
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyPaidOnCancelled, "anomaly kind")
	mustLen(t, env.store.logs, 1, "audit log")
	mustEqual(t, env.store.logs[0].FromStatus, StatusCancelled, "log from")
	mustEqual(t, env.store.logs[0].ToStatus, StatusCancelled, "log to")
	ev, ok := env.observer.firstByKind(EventAnomaly)
	if !ok {
		t.Fatal("anomaly event not found in observer")
	}
	mustEqual(t, attrString(t, ev.Attrs, "kind"), string(AnomalyPaidOnCancelled), "observer anomaly kind")
	mustEqual(t, attrString(t, ev.Attrs, "trade_no"), "TXN-CANCELLED-PAID", "observer trade_no")
	mustEqual(t, attrInt64(t, ev.Attrs, "amount"), int64(9900), "observer amount")
	mustEqual(t, attrString(t, ev.Attrs, "gateway_status"), string(TradeStatusPaid), "observer gateway_status")
}

func TestHandleNotify_CancelledPaidOnAnomalySeesAuditLog(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:       "NO-CANCELLED-HOOK",
		orderToken:    "T-CANCELLED-HOOK",
		userID:        1001,
		status:        StatusCancelled,
		payAmount:     9900,
		originalPrice: 9900,
		productTitle:  "VIP",
		payMethod:     PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-CANCELLED-HOOK", 9900, "TXN-CANCELLED-HOOK")
	env.gw.QueryResp = QueryResult{
		OutTradeNo:    "NO-CANCELLED-HOOK",
		TransactionID: "TXN-CANCELLED-HOOK",
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   9900,
		PaidAt:        time.Now(),
		Channel:       "wechat",
	}
	var logsSeenInHook int
	env.OnAnomalyHook = func(ctx context.Context, o *testOrder, kind AnomalyKind, _ string) {
		if kind != AnomalyPaidOnCancelled {
			return
		}
		logs, err := env.store.ListLogsByOrderNo(ctx, o.OrderNo())
		if err != nil {
			t.Fatalf("ListLogsByOrderNo in OnAnomaly: %v", err)
		}
		logsSeenInHook = len(logs)
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	mustEqual(t, logsSeenInHook, 1, "audit log visible inside OnAnomaly")
}

func TestHandleNotify_CancelledButGatewayQueryKeepsFailing(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:   "NO-CAN-QERR",
		status:    StatusCancelled,
		payAmount: 9900,
		payMethod: PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-CAN-QERR", 9900, "TXN")
	env.gw.QueryErr = errTestQueryDown

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	mustEqual(t, env.gw.QueryOrderCalls, 3, "Query retried 3 times")
	mustEqual(t, env.store.byNo["NO-CAN-QERR"].status, StatusCancelled, "status unchanged")
	mustEqual(t, env.store.CASReopenPaidCalls, 0, "no reopen")
	mustEqual(t, env.store.FinalizeCalls, 0, "no finalize")
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyGatewayQueryFailed, "anomaly kind")
}

func TestHandleNotify_CancelledQueryAmountMismatch(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:   "NO-CAN-AM",
		status:    StatusCancelled,
		payAmount: 9900,
		payMethod: PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-CAN-AM", 9900, "TXN")
	env.gw.QueryResp = QueryResult{
		OutTradeNo:  "NO-CAN-AM",
		TradeStatus: TradeStatusPaid,
		TotalAmount: 1,
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	mustEqual(t, env.store.byNo["NO-CAN-AM"].status, StatusCancelled, "status unchanged")
	mustEqual(t, env.store.CASReopenPaidCalls, 0, "no reopen")
	mustEqual(t, env.store.FinalizeCalls, 0, "no finalize")
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyAmountMismatch, "anomaly kind")
}

func TestHandleNotify_CancelledButGatewayReportsNotPaid(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:   "NO-CAN-UNPAID",
		status:    StatusCancelled,
		payAmount: 9900,
		payMethod: PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-CAN-UNPAID", 9900, "TXN")
	env.gw.QueryResp = QueryResult{
		OutTradeNo:  "NO-CAN-UNPAID",
		TradeStatus: TradeStatusUnpaid,
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	mustEqual(t, env.store.byNo["NO-CAN-UNPAID"].status, StatusCancelled, "status unchanged")
	mustEqual(t, env.store.CASReopenPaidCalls, 0, "no reopen")
	mustEqual(t, env.store.FinalizeCalls, 0, "no finalize")
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyPaidOnCancelled, "anomaly kind")
}

func TestHandleNotify_CASRaceFallsIntoPaidRetry(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	seedPendingOrder(env, "NO-CAS")
	env.gw.NotifyResult = makeNotify("NO-CAS", 9900, "TXN-CAS")

	// 开启竞态：CASConfirmPaid 第一次返回 0 + 把 order 改为 Paid，模拟并发抢跑。
	// 初始状态仍为 Pending，HandleNotify 的主分派走 Pending 分支 → CAS → recheck 看到 Paid → retryFinalizeForPaid
	env.store.ConfirmPaidRaceOnce = true

	mustNotErr(t, env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 交叉验证：CAS 被尝试 1 次（返回 0），不再重试 CAS，但 Finalize 被调用
	mustEqual(t, env.store.CASConfirmPaidCalls, 1, "CAS attempted once")
	mustEqual(t, env.store.FinalizeCalls, 1, "Finalize via retryFinalizeForPaid")
	mustLen(t, env.OnPaidCalls, 1, "OnPaid")
	mustEqual(t, env.store.byNo["NO-CAS"].status, StatusDelivered, "final status")
}

func TestHandleNotify_UnexpectedStatusRaisesAnomaly(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:   "NO-CAN",
		status:    OrderStatus(99),
		payAmount: 9900,
		payMethod: PayMethodWechat,
	})
	env.gw.NotifyResult = makeNotify("NO-CAN", 9900, "TXN")

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyUnexpectedStatus, "anomaly kind")
	mustEqual(t, env.store.FinalizeCalls, 0, "no Finalize")
}

// 回归安全 I2：超长字段的 notify 必须被拒（防 DB 列截断 + GetByNo 扫全表 DoS）
func TestHandleNotify_RejectsOversizedFields(t *testing.T) {
	cases := []struct {
		name   string
		notify NotifyResult
	}{
		{"empty OutTradeNo", NotifyResult{OutTradeNo: "", TradeStatus: TradeStatusPaid}},
		{"oversized OutTradeNo", NotifyResult{
			OutTradeNo:  stringOfLen(65), // max 64
			TradeStatus: TradeStatusPaid,
		}},
		{"oversized TransactionID", NotifyResult{
			OutTradeNo:    "NO-1",
			TransactionID: stringOfLen(129), // max 128
			TradeStatus:   TradeStatusPaid,
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newTestEnv(t)
			env.gw.NotifyResult = c.notify

			mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

			// 不应触发任何后续处理
			mustEqual(t, env.store.CASConfirmPaidCalls, 0, "no CAS")
			mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
		})
	}
}

// MalformedPaidNotify：Paid 状态网关回调缺关键字段，应在 GetByNo 之前拦截，
// 仅 Observer 收到 anomaly 事件，不触发 OnAnomaly 钩子，不推进状态。
func TestHandleNotify_RejectsMalformedPaidNotify_EmptyTradeNo(t *testing.T) {
	env := newTestEnv(t)
	seedPendingOrder(env, "NO-MALFORMED-1") // 落库，但 GetByNo 不应被调用

	env.gw.NotifyResult = NotifyResult{
		OutTradeNo:    "NO-MALFORMED-1",
		TransactionID: "", // 关键：空 trade no
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   9900,
		PaidAt:        time.Now(),
		Channel:       "wechat",
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 必须在 GetByNo 之前拦截
	mustEqual(t, env.store.GetByNoCalls, 0, "GetByNo not called")
	mustEqual(t, env.store.CASConfirmPaidCalls, 0, "no CAS")
	mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
	mustLen(t, env.OnAnomalyCalls, 0, "OnAnomaly hook not triggered (no order context)")

	// 但 Observer 必须收到 EventAnomaly + kind=malformed_paid_notify
	mustEqual(t, env.observer.countByKind(EventAnomaly), 1, "exactly one anomaly event")
	ev, ok := env.observer.firstByKind(EventAnomaly)
	if !ok {
		t.Fatal("anomaly event not found in observer")
	}
	if got := ev.Attrs["kind"]; got != string(AnomalyMalformedPaidNotify) {
		t.Fatalf("anomaly kind = %v, want %s", got, AnomalyMalformedPaidNotify)
	}

	// 订单仍 Pending（未受影响）
	mustEqual(t, env.store.byNo["NO-MALFORMED-1"].status, StatusPending, "order remains Pending")
}

func TestHandleNotify_RejectsMalformedPaidNotify_NonPositiveAmount(t *testing.T) {
	env := newTestEnv(t)
	seedPendingOrder(env, "NO-MALFORMED-2")

	env.gw.NotifyResult = NotifyResult{
		OutTradeNo:    "NO-MALFORMED-2",
		TransactionID: "TXN-OK",
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   0, // 关键：非正金额
		PaidAt:        time.Now(),
		Channel:       "wechat",
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	mustEqual(t, env.store.GetByNoCalls, 0, "GetByNo not called")
	mustEqual(t, env.store.CASConfirmPaidCalls, 0, "no CAS")
	mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
	mustLen(t, env.OnAnomalyCalls, 0, "OnAnomaly hook not triggered")
	mustEqual(t, env.observer.countByKind(EventAnomaly), 1, "exactly one anomaly event")

	ev, _ := env.observer.firstByKind(EventAnomaly)
	if got := ev.Attrs["kind"]; got != string(AnomalyMalformedPaidNotify) {
		t.Fatalf("anomaly kind = %v, want %s", got, AnomalyMalformedPaidNotify)
	}

	mustEqual(t, env.store.byNo["NO-MALFORMED-2"].status, StatusPending, "order remains Pending")
}

func TestHandleNotify_OnPaidFailureKeepsOrderInPaid(t *testing.T) {
	env := newTestEnv(t)
	seedPendingOrder(env, "NO-FAIL")
	env.gw.NotifyResult = makeNotify("NO-FAIL", 9900, "TXN-FAIL")
	// OnPaid 钩子报错（业务侧权益服务挂了）
	env.OnPaidErr = errTestVIPDown

	// Engine 对网关应该返回 nil，避免重试风暴
	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 订单停在 Paid（未进入 Delivered），等 fallback scanner 重试
	mustEqual(t, env.store.byNo["NO-FAIL"].status, StatusPaid, "stuck at Paid")
	mustEqual(t, env.store.FinalizeCalls, 0, "Finalize not called")
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly delivery failed")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyDeliveryFailed, "anomaly kind")
}

// recheckAfterCASFailed 的其他分支覆盖——通过 ConfirmPaidRaceToStatus 注入不同状态
func TestHandleNotify_RecheckSeesClosedAfterCASMiss(t *testing.T) {
	env := newTestEnv(t)
	seedPendingOrder(env, "NO-CL-R")
	env.gw.NotifyResult = makeNotify("NO-CL-R", 9900, "TXN")
	// 注入竞态：CAS 返回 0，状态改为 Closed → 走 handleClosedPaidNotify
	closedStatus := StatusClosed
	env.store.ConfirmPaidRaceToStatus = &closedStatus
	// 让网关 Query 确认已付，测试能正常恢复
	env.gw.QueryResp = QueryResult{
		OutTradeNo: "NO-CL-R", TransactionID: "TXN",
		TradeStatus: TradeStatusPaid, TotalAmount: 9900,
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 走了 handleClosedPaidNotify 路径并成功恢复
	mustEqual(t, env.store.CASReopenPaidCalls, 1, "CASReopenPaid attempted")
	mustEqual(t, env.store.byNo["NO-CL-R"].status, StatusDelivered, "finalized via reopen")
	mustLen(t, env.OnReopenedCalls, 1, "OnReopened fired")
}

func TestHandleNotify_RecheckSeesDeliveredAfterCASMiss(t *testing.T) {
	env := newTestEnv(t)
	seedPendingOrder(env, "NO-DL-R")
	env.gw.NotifyResult = makeNotify("NO-DL-R", 9900, "TXN")
	deliveredStatus := StatusDelivered
	env.store.ConfirmPaidRaceToStatus = &deliveredStatus

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// Delivered 分支——仅记日志，不做其他
	mustEqual(t, env.store.byNo["NO-DL-R"].status, StatusDelivered, "unchanged")
	mustEqual(t, env.store.FinalizeCalls, 0, "no new finalize")
	mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
	mustLen(t, env.OnAnomalyCalls, 0, "no anomaly")
}

func TestHandleNotify_RecheckSeesCancelledAfterCASMiss(t *testing.T) {
	env := newTestEnv(t)
	seedPendingOrder(env, "NO-CN-R")
	env.gw.NotifyResult = makeNotify("NO-CN-R", 9900, "TXN")
	cancelledStatus := StatusCancelled
	env.store.ConfirmPaidRaceToStatus = &cancelledStatus
	env.gw.QueryResp = QueryResult{
		OutTradeNo:    "NO-CN-R",
		TransactionID: "TXN",
		TradeStatus:   TradeStatusPaid,
		TotalAmount:   9900,
	}

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	mustEqual(t, env.gw.QueryOrderCalls, 1, "QueryOrder called")
	mustEqual(t, env.store.byNo["NO-CN-R"].status, StatusCancelled, "status unchanged")
	mustEqual(t, env.store.CASReopenPaidCalls, 0, "no reopen")
	mustEqual(t, env.store.FinalizeCalls, 0, "no finalize")
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly fired")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyPaidOnCancelled, "anomaly kind")
}

func TestHandleNotify_RecheckSeesDisappearedAfterCASMiss(t *testing.T) {
	env := newTestEnv(t)
	seedPendingOrder(env, "NO-GONE-R")
	env.gw.NotifyResult = makeNotify("NO-GONE-R", 9900, "TXN")
	env.store.ConfirmPaidMakeDisappearOnce = true

	mustNotErr(t, env.engine.HandleNotify(context.Background(), "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// 订单消失——engine 只记 ALERT 日志，不做其他动作（没有 order 实例传给 OnAnomaly）
	mustEqual(t, env.store.FinalizeCalls, 0, "no finalize")
	mustLen(t, env.OnPaidCalls, 0, "no OnPaid")
}

var errTestVIPDown = &testHookError{msg: "vip service down"}

type testHookError struct{ msg string }

func (e *testHookError) Error() string { return e.msg }
