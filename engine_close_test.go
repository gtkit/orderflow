package orderflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Close 的交叉验证点：
//   - 状态前置：只有 Pending 且已过期才推进到 Closed
//   - 网关：CloseOrder 调用 + 错误容忍（IsIgnorableCloseError）
//   - Store：CAS + AppendLog
//   - Cache + Stream：Closed 状态推送
//   - Hook：OnClosed + reason = Timeout

func TestClose_HappyPath_ExpiredPending(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	env.store.seed(&testOrder{
		orderNo:    "NO-1",
		orderToken: "T-1",
		userID:     1001,
		status:     StatusPending,
		payMethod:  PayMethodWechat,
		expireAt:   time.Now().Add(-time.Minute), // 已过期
	})

	mustNotErr(t, env.engine.Close(ctx, "NO-1"), "Close")

	// 订单已关闭
	mustEqual(t, env.store.byNo["NO-1"].status, StatusClosed, "final status")

	// 网关 + CAS + Log 都正确
	mustEqual(t, env.gw.CloseOrderCalls, 1, "Gateway.CloseOrderCalls")
	mustEqual(t, env.store.CASCloseCalls, 1, "CASClose calls")
	mustEqual(t, env.store.AppendLogCalls, 1, "AppendLog calls")
	mustEqual(t, env.store.logs[0].ToStatus, StatusClosed, "log ToStatus")

	// Cache + Stream
	mustEqual(t, env.cache.SetCalls, 1, "cache Set")
	mustEqual(t, env.cache.SetHistory[0].Status, StatusClosed, "cache status")
	mustLen(t, env.stream.Published, 1, "stream published")
	mustEqual(t, env.stream.Published[0].Status, StatusClosed, "stream status")

	// Hook：OnClosed 触发，reason = Timeout
	mustLen(t, env.OnClosedCalls, 1, "OnClosed")
	mustEqual(t, env.OnClosedCalls[0].Reason, ClosedReasonTimeout, "OnClosed reason")
}

func TestClose_SkipsNotFound(t *testing.T) {
	env := newTestEnv(t)
	mustNotErr(t, env.engine.Close(context.Background(), "UNKNOWN"), "Close (not found)")

	// 副作用零
	mustEqual(t, env.gw.CloseOrderCalls, 0, "no gateway close")
	mustEqual(t, env.store.CASCloseCalls, 0, "no CAS")
	mustEqual(t, env.store.AppendLogCalls, 0, "no log")
	mustLen(t, env.OnClosedCalls, 0, "no hook")
}

func TestClose_SkipsNonPending(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo: "NO-PAID",
		status:  StatusPaid,
	})
	mustNotErr(t, env.engine.Close(context.Background(), "NO-PAID"), "Close (paid)")

	mustEqual(t, env.gw.CloseOrderCalls, 0, "no gateway close")
	mustEqual(t, env.store.CASCloseCalls, 0, "no CAS")
}

func TestClose_SkipsNotExpired(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:  "NO-FUTURE",
		status:   StatusPending,
		expireAt: time.Now().Add(time.Hour),
	})
	mustNotErr(t, env.engine.Close(context.Background(), "NO-FUTURE"), "Close (not expired)")

	mustEqual(t, env.gw.CloseOrderCalls, 0, "no gateway close")
	mustEqual(t, env.store.CASCloseCalls, 0, "no CAS")
}

func TestClose_TolerantOfIgnorableGatewayError(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// 网关返回错误，但被识别为可忽略（例如订单不存在 / 已关闭）
	env.gw.CloseOrderErr = errors.New("gateway: order not found")
	env.gw.IgnorableFn = func(_ Channel, _ error) bool { return true }

	env.store.seed(&testOrder{
		orderNo:    "NO-IG",
		orderToken: "T-IG",
		status:     StatusPending,
		expireAt:   time.Now().Add(-time.Minute),
	})

	mustNotErr(t, env.engine.Close(ctx, "NO-IG"), "Close (ignorable gateway err)")
	mustEqual(t, env.store.byNo["NO-IG"].status, StatusClosed, "final status")
}

func TestClose_NonIgnorableGatewayErrorBlocks(t *testing.T) {
	env := newTestEnv(t)
	env.gw.CloseOrderErr = errors.New("gateway: timeout")
	// IgnorableFn 默认返回 false

	env.store.seed(&testOrder{
		orderNo:  "NO-ERR",
		status:   StatusPending,
		expireAt: time.Now().Add(-time.Minute),
	})

	err := env.engine.Close(context.Background(), "NO-ERR")
	if err == nil {
		t.Fatal("expected error from Close on non-ignorable gateway err")
	}

	// 订单未关闭
	mustEqual(t, env.store.byNo["NO-ERR"].status, StatusPending, "still Pending")
	mustEqual(t, env.store.CASCloseCalls, 0, "CAS not attempted")

	// 网关重试 3 次（带 retryN）
	mustEqual(t, env.gw.CloseOrderCalls, 3, "gateway retried 3x")

	// 失败日志写入
	mustEqual(t, env.store.AppendLogCalls, 1, "failure log appended")
}

func TestClose_CASRaceLostToPaid(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	env.store.seed(&testOrder{
		orderNo:   "NO-RACE",
		status:    StatusPending,
		expireAt:  time.Now().Add(-time.Minute),
		payMethod: PayMethodWechat,
	})
	// 网关 Close 成功，但 CAS 时被支付回调抢先
	env.store.CASCloseLosesToPaidOnce = true

	mustNotErr(t, env.engine.Close(ctx, "NO-RACE"), "Close (CAS race)")

	// 最终状态应该是 Paid（race 改写），不是 Closed
	mustEqual(t, env.store.byNo["NO-RACE"].status, StatusPaid, "order transitioned to Paid")
	// 收录了"payment won race"日志
	found := false
	for _, l := range env.store.logs {
		if l.Remark == "payment won race during timeout close" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected race log, got %+v", env.store.logs)
	}

	// OnClosed 不触发（因为 CAS affected=0，没真的 close 成功）
	mustLen(t, env.OnClosedCalls, 0, "OnClosed (race, not closed)")
}

// ReconcilePaid 场景
func TestReconcilePaid_HappyPath(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	paidAt := time.Now().Add(-time.Minute)
	env.store.seed(&testOrder{
		orderNo:       "NO-RP",
		orderToken:    "T-RP",
		userID:        1001,
		status:        StatusPaid,
		productID:     2001,
		productTitle:  "VIP",
		originalPrice: 9900,
		payAmount:     9900,
		payMethod:     PayMethodWechat,
		tradeNo:       "TXN-RP",
		paidAt:        &paidAt,
	})

	mustNotErr(t, env.engine.ReconcilePaid(ctx, "NO-RP"), "ReconcilePaid")

	// 订单推进到 Delivered
	mustEqual(t, env.store.byNo["NO-RP"].status, StatusDelivered, "final status")

	// OnPaid + Finalize + OnDelivered 按序触发
	mustLen(t, env.OnPaidCalls, 1, "OnPaid")
	mustEqual(t, env.OnPaidCalls[0].TradeNo, "TXN-RP", "OnPaid tradeNo")
	mustEqual(t, env.store.FinalizeCalls, 1, "Finalize")
	mustLen(t, env.store.bills, 1, "bill inserted")
	mustLen(t, env.OnDeliveredCalls, 1, "OnDelivered")

	// 缓存 Delivered 已推送
	lastSet := env.cache.SetHistory[len(env.cache.SetHistory)-1]
	mustEqual(t, lastSet.Status, StatusDelivered, "final cache status")
}

func TestReconcilePaid_SkipsAlreadyDelivered(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{orderNo: "NO-D", status: StatusDelivered})
	mustNotErr(t, env.engine.ReconcilePaid(context.Background(), "NO-D"), "ReconcilePaid")
	mustLen(t, env.OnPaidCalls, 0, "OnPaid not called")
	mustEqual(t, env.store.FinalizeCalls, 0, "no Finalize")
}

func TestReconcilePaid_SkipsNonPaidStatus(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{orderNo: "NO-P", status: StatusPending, expireAt: time.Now().Add(time.Hour)})
	mustNotErr(t, env.engine.ReconcilePaid(context.Background(), "NO-P"), "ReconcilePaid")
	mustLen(t, env.OnPaidCalls, 0, "OnPaid not called")
}

func TestReconcilePaid_ErrorsOnMissingMetadata(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo: "NO-NP",
		status:  StatusPaid,
		// 缺少 tradeNo 和 paidAt
	})
	err := env.engine.ReconcilePaid(context.Background(), "NO-NP")
	if err == nil {
		t.Fatal("expected error due to missing metadata")
	}
}

func TestReconcilePaid_HookFailureBubbles(t *testing.T) {
	env := newTestEnv(t)
	env.OnPaidErr = errors.New("vip service down")

	paidAt := time.Now()
	env.store.seed(&testOrder{
		orderNo:   "NO-FAIL",
		status:    StatusPaid,
		tradeNo:   "TXN",
		paidAt:    &paidAt,
		payMethod: PayMethodWechat,
	})
	err := env.engine.ReconcilePaid(context.Background(), "NO-FAIL")
	if err == nil {
		t.Fatal("expected error when OnPaid fails")
	}

	// 订单仍在 Paid（未被推进）
	mustEqual(t, env.store.byNo["NO-FAIL"].status, StatusPaid, "status unchanged")
	// FinalizePaidOrder 未被调用
	mustEqual(t, env.store.FinalizeCalls, 0, "no Finalize")
	// OnAnomaly 记录了失败
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyDeliveryFailed, "anomaly kind")
}

func TestFindExpiredPending_PassthroughToStore(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	now := time.Now()
	env.store.seed(&testOrder{orderNo: "A", status: StatusPending, expireAt: now.Add(-time.Hour)})
	env.store.seed(&testOrder{orderNo: "B", status: StatusPending, expireAt: now.Add(-time.Minute)})
	env.store.seed(&testOrder{orderNo: "C", status: StatusPending, expireAt: now.Add(time.Hour)})
	env.store.seed(&testOrder{orderNo: "D", status: StatusPaid})

	got, err := env.engine.FindExpiredPending(ctx, 10)
	mustNotErr(t, err, "FindExpiredPending")
	mustLen(t, got, 2, "expired pending count")
}

func TestFindPaidUndelivered_PassthroughToStore(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{orderNo: "A", status: StatusPaid})
	env.store.seed(&testOrder{orderNo: "B", status: StatusDelivered})
	env.store.seed(&testOrder{orderNo: "C", status: StatusPaid})

	got, err := env.engine.FindPaidUndelivered(context.Background(), 10)
	mustNotErr(t, err, "FindPaidUndelivered")
	mustLen(t, got, 2, "paid undelivered count")
}

// =============================================================================
// CloseByAdmin —— 强制关单（绕过 ExpireAt）
// =============================================================================

func TestCloseByAdmin_ClosesUnexpiredPending(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	env.store.seed(&testOrder{
		orderNo:    "ADM-1",
		orderToken: "T-ADM",
		userID:     1001,
		status:     StatusPending,
		payMethod:  PayMethodWechat,
		expireAt:   time.Now().Add(time.Hour), // 未过期——标准 Close 会跳过
	})

	mustNotErr(t, env.engine.CloseByAdmin(ctx, "ADM-1", "fraud:rule-42"), "CloseByAdmin")
	mustEqual(t, env.store.byNo["ADM-1"].status, StatusClosed, "final status")
	mustEqual(t, env.gw.CloseOrderCalls, 1, "Gateway.CloseOrderCalls")
	mustEqual(t, env.store.CASCloseCalls, 1, "CASClose calls")
	mustLen(t, env.OnClosedCalls, 1, "OnClosed")
	mustEqual(t, env.OnClosedCalls[0].Reason, ClosedReasonManual, "OnClosed reason")
	// 流水带 admin actor + reason
	mustEqual(t, env.store.logs[0].Actor, "admin", "log actor")
}

func TestCloseByAdmin_SkipsNonPending(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	env.store.seed(&testOrder{orderNo: "ADM-2", status: StatusPaid, expireAt: time.Now().Add(time.Hour)})
	mustNotErr(t, env.engine.CloseByAdmin(ctx, "ADM-2", "any"), "CloseByAdmin should skip Paid")
	mustEqual(t, env.store.byNo["ADM-2"].status, StatusPaid, "status unchanged")
	mustEqual(t, env.gw.CloseOrderCalls, 0, "no gateway call")
}

func TestCloseByAdmin_NotFoundReturnsErrOrderNotFound(t *testing.T) {
	env := newTestEnv(t)
	if err := env.engine.CloseByAdmin(context.Background(), "MISSING", ""); !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("want ErrOrderNotFound, got %v", err)
	}
}

func TestClampLimit(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{-1, MaxFindLimit},
		{0, MaxFindLimit},
		{1, 1},
		{MaxFindLimit - 1, MaxFindLimit - 1},
		{MaxFindLimit, MaxFindLimit},
		{MaxFindLimit + 1, MaxFindLimit},
		{1 << 30, MaxFindLimit}, // 防御极端输入（1B）
	}
	for _, c := range cases {
		if got := clampLimit(c.in); got != c.want {
			t.Errorf("clampLimit(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

// =============================================================================
// CancelByUser
// =============================================================================

func TestCancelByUser_HappyPath(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	env.store.seed(&testOrder{
		orderNo:    "C-1",
		orderToken: "TC-1",
		userID:     1001,
		status:     StatusPending,
		payMethod:  PayMethodWechat,
		expireAt:   time.Now().Add(time.Hour), // 未过期也能取消
	})

	mustNotErr(t, env.engine.CancelByUser(ctx, 1001, "C-1", "switch_payment"), "CancelByUser")

	mustEqual(t, env.store.byNo["C-1"].status, StatusCancelled, "final status")
	mustEqual(t, env.gw.CloseOrderCalls, 1, "Gateway.CloseOrder")
	mustEqual(t, env.store.CASCancelCalls, 1, "CASCancel calls")
	mustEqual(t, env.store.AppendLogCalls, 1, "AppendLog calls")
	mustEqual(t, env.store.logs[0].ToStatus, StatusCancelled, "log ToStatus")
	mustEqual(t, env.store.logs[0].Actor, "user", "log actor")

	mustEqual(t, env.cache.SetCalls, 1, "cache Set")
	mustEqual(t, env.cache.SetHistory[0].Status, StatusCancelled, "cache status")
	mustLen(t, env.stream.Published, 1, "stream published")
	mustEqual(t, env.stream.Published[0].Status, StatusCancelled, "stream status")

	mustLen(t, env.OnCancelledCalls, 1, "OnCancelled hook")
	mustEqual(t, env.OnCancelledCalls[0].Reason, "switch_payment", "reason transparent")
	mustLen(t, env.OnClosedCalls, 0, "OnClosed must NOT fire on cancel path")
}

func TestCancelByUser_RejectsForeignUser(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo: "C-2",
		userID:  1001,
		status:  StatusPending,
	})

	err := env.engine.CancelByUser(context.Background(), 9999, "C-2", "")
	if !errors.Is(err, ErrOrderForbidden) {
		t.Fatalf("CancelByUser foreign user: got %v, want ErrOrderForbidden", err)
	}

	mustEqual(t, env.store.CASCancelCalls, 0, "no CAS")
	mustEqual(t, env.gw.CloseOrderCalls, 0, "no gateway close")
	mustLen(t, env.OnCancelledCalls, 0, "no hook")
}

func TestCancelByUser_NotFound(t *testing.T) {
	env := newTestEnv(t)
	err := env.engine.CancelByUser(context.Background(), 1001, "UNKNOWN", "")
	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("CancelByUser not found: got %v, want ErrOrderNotFound", err)
	}
}

func TestCancelByUser_SkipsNonPending(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo: "C-3",
		userID:  1001,
		status:  StatusPaid, // 已支付，不能再取消
	})

	mustNotErr(t, env.engine.CancelByUser(context.Background(), 1001, "C-3", ""), "CancelByUser paid")

	mustEqual(t, env.store.byNo["C-3"].status, StatusPaid, "status unchanged")
	mustEqual(t, env.store.CASCancelCalls, 0, "no CAS on paid")
	mustEqual(t, env.gw.CloseOrderCalls, 0, "no gateway on paid")
	mustLen(t, env.OnCancelledCalls, 0, "no hook on paid")
}

// delay-queue-cleanup-consistency：CancelByUser 成功路径必须清理延时队列。
// 高频取消场景下漏 Remove 会让 CloseWorker 拉到过期任务再幂等 skip，浪费资源 + 污染日志。
func TestCancelByUser_RemovesFromDelayQueue(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:    "C-DQ",
		orderToken: "T-CDQ",
		userID:     1001,
		status:     StatusPending,
		payMethod:  PayMethodWechat,
		expireAt:   time.Now().Add(time.Hour),
	})
	env.dq.enqueued["C-DQ"] = time.Now().Add(time.Hour)

	mustNotErr(t, env.engine.CancelByUser(context.Background(), 1001, "C-DQ", ""), "CancelByUser")

	mustEqual(t, env.dq.RemoveCalls, 1, "delay queue Remove called once")
	if _, stillEnqueued := env.dq.enqueued["C-DQ"]; stillEnqueued {
		t.Fatal("expected order removed from delay queue")
	}
}

// CancelByUser 路径下 Remove 失败必须触发 anomaly（不阻断主流程）。
func TestCancelByUser_DelayQueueRemoveFailureEmitsAnomaly(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:    "C-DQ-ERR",
		orderToken: "T-CDQE",
		userID:     1001,
		status:     StatusPending,
		payMethod:  PayMethodWechat,
		expireAt:   time.Now().Add(time.Hour),
	})
	env.dq.ErrOnRemove = errors.New("redis down")

	mustNotErr(t, env.engine.CancelByUser(context.Background(), 1001, "C-DQ-ERR", ""), "CancelByUser")

	// 主流程仍然推进到 Cancelled
	mustEqual(t, env.store.byNo["C-DQ-ERR"].status, StatusCancelled, "cancelled despite queue error")
	// anomaly 触发
	mustLen(t, env.OnAnomalyCalls, 1, "OnAnomaly fired")
	mustEqual(t, env.OnAnomalyCalls[0].Kind, AnomalyDelayQueueCleanupFailed, "anomaly kind")
}

func TestCancelByUser_ReturnsAlreadyPaidWhenPaymentWinsRace(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:   "C-PAID-RACE",
		userID:    1001,
		status:    StatusPending,
		payMethod: PayMethodWechat,
		expireAt:  time.Now().Add(time.Hour),
	})
	env.store.CancelRaceToStatus = ptrStatus(StatusPaid)

	err := env.engine.CancelByUser(context.Background(), 1001, "C-PAID-RACE", "switch_payment")
	if !errors.Is(err, ErrOrderAlreadyPaid) {
		t.Fatalf("CancelByUser race: got %v, want ErrOrderAlreadyPaid", err)
	}
	mustLen(t, env.store.logs, 1, "race audit log")
	mustEqual(t, env.store.logs[0].ToStatus, StatusPaid, "log to paid")
	mustLen(t, env.OnCancelledCalls, 0, "no cancelled hook")
}

func TestCancelByUser_CancelledRaceRemainsIdempotent(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:   "C-CAN-RACE",
		userID:    1001,
		status:    StatusPending,
		payMethod: PayMethodWechat,
		expireAt:  time.Now().Add(time.Hour),
	})
	env.store.CancelRaceToStatus = ptrStatus(StatusCancelled)

	mustNotErr(t, env.engine.CancelByUser(context.Background(), 1001, "C-CAN-RACE", ""), "CancelByUser cancelled race")
	mustLen(t, env.store.logs, 1, "race audit log")
	mustEqual(t, env.store.logs[0].ToStatus, StatusCancelled, "log to cancelled")
	mustLen(t, env.OnCancelledCalls, 0, "no hook for race loser")
}

// delay-queue-cleanup-consistency：CloseByAdmin 成功路径必须清理延时队列。
func TestCloseByAdmin_RemovesFromDelayQueue(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:    "ADM-DQ",
		orderToken: "T-ADM-DQ",
		userID:     1001,
		status:     StatusPending,
		payMethod:  PayMethodWechat,
		expireAt:   time.Now().Add(time.Hour),
	})
	env.dq.enqueued["ADM-DQ"] = time.Now().Add(time.Hour)

	mustNotErr(t, env.engine.CloseByAdmin(context.Background(), "ADM-DQ", "fraud"), "CloseByAdmin")

	mustEqual(t, env.dq.RemoveCalls, 1, "delay queue Remove called once")
	if _, stillEnqueued := env.dq.enqueued["ADM-DQ"]; stillEnqueued {
		t.Fatal("expected order removed from delay queue")
	}
}

// =============================================================================
// CASConfirmPaid 二级金额校验（fakeStore 模拟 DB 列值）
// =============================================================================

func TestCASConfirmPaid_RejectsAmountMismatch(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo:   "A-1",
		userID:    1001,
		status:    StatusPending,
		payAmount: 9900, // DB 上是 9900
		expireAt:  time.Now().Add(time.Hour),
	})

	// 错金额：CAS 应返回 affected=0，订单仍在 Pending
	affected, err := env.store.CASConfirmPaid(context.Background(), "A-1", "TXN-WRONG", time.Now(), 1234)
	mustNotErr(t, err, "CASConfirmPaid")
	mustEqual(t, affected, int64(0), "affected for amount mismatch")
	mustEqual(t, env.store.byNo["A-1"].status, StatusPending, "status unchanged")

	// 对金额：CAS 推进
	affected, err = env.store.CASConfirmPaid(context.Background(), "A-1", "TXN-OK", time.Now(), 9900)
	mustNotErr(t, err, "CASConfirmPaid match")
	mustEqual(t, affected, int64(1), "affected for matching amount")
	mustEqual(t, env.store.byNo["A-1"].status, StatusPaid, "status advanced")
}
