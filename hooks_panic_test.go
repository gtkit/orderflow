package orderflow

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// 业务钩子 panic 不应冲破主流程：HandleNotify 必须正常返回，订单状态推进到 Paid，
// observer 收到 hook_panic 异常事件。
func TestHook_OnPaidPanicDoesNotCrashHandleNotify(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	env.store.seed(&testOrder{
		orderNo:       "PNC-1",
		orderToken:    "T-PNC",
		userID:        1001,
		status:        StatusPending,
		productID:     2001,
		productTitle:  "VIP",
		originalPrice: 9900,
		payAmount:     9900,
		payMethod:     PayMethodWechat,
		expireAt:      time.Now().Add(time.Hour),
	})

	env.engine.onPaid = func(_ context.Context, _ *testOrder, _ NotifyResult) error {
		panic("OnPaid blew up")
	}

	rec := newRecordingObserver()
	env.engine.observer = rec

	env.gw.NotifyResult = makeNotify("PNC-1", 9900, "TXN-PNC")

	// 不应 panic、不应返回 error（HandleNotify 已经 ack notify、补偿走 fallback）
	if err := env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()); err != nil {
		t.Fatalf("HandleNotify should swallow OnPaid panic, got error: %v", err)
	}

	// CAS 已成功（订单 Paid 状态）
	mustEqual(t, env.store.byNo["PNC-1"].status, StatusPaid, "order should be Paid (CAS succeeded before hook)")

	// observer 收到 hook_panic 事件
	hookPanics := 0
	for _, ev := range rec.byKind(EventAnomaly) {
		if ev.Attrs["kind"] == "hook_panic" {
			hookPanics++
		}
	}
	if hookPanics == 0 {
		t.Errorf("expected hook_panic anomaly event, got none")
	}
}

func TestHook_OnClosedPanicDoesNotCrashClose(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	env.store.seed(&testOrder{
		orderNo:    "PNC-2",
		orderToken: "T-PNC2",
		status:     StatusPending,
		payMethod:  PayMethodWechat,
		expireAt:   time.Now().Add(-time.Minute),
	})

	env.engine.onClosed = func(_ context.Context, _ *testOrder, _ ClosedReason) {
		panic("OnClosed blew up")
	}

	if err := env.engine.Close(ctx, "PNC-2"); err != nil {
		t.Fatalf("Close should swallow OnClosed panic, got error: %v", err)
	}
	mustEqual(t, env.store.byNo["PNC-2"].status, StatusClosed, "order should be Closed")
}

func TestHook_OnAnomalyPanicDoesNotCrashHandleNotify(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	env.store.seed(&testOrder{
		orderNo:    "PNC-3",
		orderToken: "T-PNC3",
		userID:     1001,
		status:     StatusPending,
		productID:  2001,
		payAmount:  9900,
		payMethod:  PayMethodWechat,
		expireAt:   time.Now().Add(time.Hour),
	})

	env.engine.onAnomaly = func(_ context.Context, _ *testOrder, _ AnomalyKind, _ string) {
		panic("OnAnomaly blew up")
	}

	// 制造金额不一致触发 anomaly 路径
	env.gw.NotifyResult = makeNotify("PNC-3", 1, "TXN-PNC3")

	if err := env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()); err != nil {
		t.Fatalf("HandleNotify should swallow OnAnomaly panic, got %v", err)
	}
	// 状态保持 Pending（没 CAS 推进）
	mustEqual(t, env.store.byNo["PNC-3"].status, StatusPending, "order should stay Pending")
}

// safeHookE 把 panic 转成 error
func TestSafeHookE_TurnsPanicIntoError(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	err := env.engine.safeHookE(ctx, "Test", "ORD", func() error {
		panic("kaboom")
	})
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("safeHookE should return error mentioning panic, got %v", err)
	}
}

// safeHookE 正常路径透传 error
func TestSafeHookE_PropagatesError(t *testing.T) {
	env := newTestEnv(t)
	want := errors.New("biz error")
	got := env.engine.safeHookE(context.Background(), "Test", "ORD", func() error { return want })
	if !errors.Is(got, want) {
		t.Errorf("safeHookE should propagate error, got %v", got)
	}
}

// 用占位让 httptest 引用编译通过（后续如需 HTTP 请求会用到）
var _ = httptest.NewRecorder
