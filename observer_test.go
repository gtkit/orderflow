package orderflow

import (
	"context"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// recordingObserver —— 用于测试的 Observer 实现
// =============================================================================

type recordedEvent struct {
	Kind    EventKind
	OrderNo string
	Attrs   map[string]any
}

type recordedDuration struct {
	Op    string
	Took  time.Duration
	Error error
}

type recordingObserver struct {
	mu        sync.Mutex
	Events    []recordedEvent
	Durations []recordedDuration
}

func newRecordingObserver() *recordingObserver { return &recordingObserver{} }

func (r *recordingObserver) Event(_ context.Context, kind EventKind, orderNo string, attrs map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make(map[string]any, len(attrs))
	for k, v := range attrs {
		copied[k] = v
	}
	r.Events = append(r.Events, recordedEvent{Kind: kind, OrderNo: orderNo, Attrs: copied})
}

func (r *recordingObserver) Duration(_ context.Context, op string, d time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Durations = append(r.Durations, recordedDuration{Op: op, Took: d, Error: err})
}

func (r *recordingObserver) byKind(kind EventKind) []recordedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []recordedEvent
	for _, e := range r.Events {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

func (r *recordingObserver) durationsByOp(op string) []recordedDuration {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []recordedDuration
	for _, d := range r.Durations {
		if d.Op == op {
			out = append(out, d)
		}
	}
	return out
}

// newTestEnvWithObserver 和 newTestEnv 类似，但额外注入 recordingObserver。
func newTestEnvWithObserver(t *testing.T) (*testEnv, *recordingObserver) {
	t.Helper()
	env := newTestEnv(t)
	obs := newRecordingObserver()
	env.engine.observer = obs
	return env, obs
}

// =============================================================================
// 专项测试
// =============================================================================

func TestObserver_FiresOnSuccessfulCreate(t *testing.T) {
	env, obs := newTestEnvWithObserver(t)
	ctx := context.Background()

	result, err := env.engine.Create(ctx, standardRequest())
	mustNotErr(t, err, "Create")

	// Duration
	durs := obs.durationsByOp(OpCreate)
	mustLen(t, durs, 1, "Create duration")
	if durs[0].Took <= 0 {
		t.Errorf("Create duration should be > 0, got %s", durs[0].Took)
	}
	if durs[0].Error != nil {
		t.Errorf("success path duration.Error = %v, want nil", durs[0].Error)
	}

	// Event
	created := obs.byKind(EventOrderCreated)
	mustLen(t, created, 1, "Created events")
	mustEqual(t, created[0].OrderNo, result.Order.OrderNo(), "event orderNo")
	if uid, _ := created[0].Attrs["user_id"].(int64); uid != result.Order.UserID() {
		t.Errorf("event user_id = %v, want %d", created[0].Attrs["user_id"], result.Order.UserID())
	}
}

func TestObserver_FiresOnReusedCreate(t *testing.T) {
	env, obs := newTestEnvWithObserver(t)
	ctx := context.Background()
	req := standardRequest()

	env.store.seed(&testOrder{
		orderNo: "REUSE", orderToken: "T-REUSE", userID: req.UserID,
		status: StatusPending, productID: req.Product.ID,
		originalPrice: req.Product.Price, payAmount: req.Product.Price,
		payMethod: req.PayMethod, expireAt: time.Now().Add(time.Hour),
	})

	_, err := env.engine.Create(ctx, req)
	mustNotErr(t, err, "Create")

	reused := obs.byKind(EventOrderReused)
	mustLen(t, reused, 1, "Reused events")
	mustEqual(t, reused[0].OrderNo, "REUSE", "event orderNo")

	// 复用场景不应触发 Created
	mustLen(t, obs.byKind(EventOrderCreated), 0, "no Created event on reuse")
}

func TestObserver_FiresOnErroredCreate(t *testing.T) {
	env, obs := newTestEnvWithObserver(t)
	req := standardRequest()
	req.UserID = 0 // invalid

	_, _ = env.engine.Create(context.Background(), req)

	durs := obs.durationsByOp(OpCreate)
	mustLen(t, durs, 1, "Create duration (error path)")
	if durs[0].Error == nil {
		t.Error("error path duration.Error should be non-nil")
	}
	// 失败早期——不应有 Created event
	mustLen(t, obs.byKind(EventOrderCreated), 0, "no Created event on early failure")
}

func TestObserver_FullPaymentFlowEvents(t *testing.T) {
	env, obs := newTestEnvWithObserver(t)
	ctx := context.Background()

	// Seed Pending 订单 + 构造 notify
	env.store.seed(&testOrder{
		orderNo: "FLOW-1", orderToken: "T-FLOW", userID: 1001,
		status: StatusPending, productID: 2001, productTitle: "VIP",
		originalPrice: 9900, payAmount: 9900, payMethod: "wechat",
		expireAt: time.Now().Add(time.Hour),
	})
	env.gw.NotifyResult = makeNotify("FLOW-1", 9900, "TXN-FLOW")

	mustNotErr(t, env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	// HandleNotify duration 记录了一次（无错）
	durs := obs.durationsByOp(OpHandleNotify)
	mustLen(t, durs, 1, "HandleNotify duration")
	if durs[0].Error != nil {
		t.Errorf("HandleNotify error = %v", durs[0].Error)
	}

	// 事件序列：Paid → Delivered
	paid := obs.byKind(EventOrderPaid)
	delivered := obs.byKind(EventOrderDelivered)
	mustLen(t, paid, 1, "Paid events")
	mustLen(t, delivered, 1, "Delivered events")
	mustEqual(t, paid[0].OrderNo, "FLOW-1", "paid orderNo")
	mustEqual(t, delivered[0].OrderNo, "FLOW-1", "delivered orderNo")
	if tn, _ := paid[0].Attrs["trade_no"].(string); tn != "TXN-FLOW" {
		t.Errorf("paid trade_no attr = %v", paid[0].Attrs["trade_no"])
	}
}

func TestObserver_FiresOnClosed(t *testing.T) {
	env, obs := newTestEnvWithObserver(t)
	ctx := context.Background()

	env.store.seed(&testOrder{
		orderNo: "CLS", orderToken: "T-CLS", userID: 1001,
		status: StatusPending, payMethod: "wechat",
		expireAt: time.Now().Add(-time.Minute), // already expired
	})
	mustNotErr(t, env.engine.Close(ctx, "CLS"), "Close")

	closed := obs.byKind(EventOrderClosed)
	mustLen(t, closed, 1, "Closed events")
	mustEqual(t, closed[0].OrderNo, "CLS", "closed orderNo")
	if reason, _ := closed[0].Attrs["reason"].(string); reason != string(ClosedReasonTimeout) {
		t.Errorf("closed reason = %v", closed[0].Attrs["reason"])
	}

	mustLen(t, obs.durationsByOp(OpClose), 1, "Close duration")
}

func TestObserver_FiresOnAnomaly(t *testing.T) {
	env, obs := newTestEnvWithObserver(t)
	ctx := context.Background()

	env.store.seed(&testOrder{
		orderNo: "AM", orderToken: "T-AM", userID: 1001,
		status: StatusPending, productID: 2001,
		payAmount: 9900, payMethod: "wechat",
		expireAt: time.Now().Add(time.Hour),
	})
	env.gw.NotifyResult = makeNotify("AM", 1, "TXN-AM") // 金额错配

	mustNotErr(t, env.engine.HandleNotify(ctx, "wechat", makeHTTPNotifyRequest()), "HandleNotify")

	anoms := obs.byKind(EventAnomaly)
	mustLen(t, anoms, 1, "Anomaly events")
	if kind, _ := anoms[0].Attrs["kind"].(string); kind != string(AnomalyAmountMismatch) {
		t.Errorf("anomaly kind = %v", anoms[0].Attrs["kind"])
	}
}

// =============================================================================
// CloseByUser 归属校验
// =============================================================================

func TestCloseByUser_Success(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.store.seed(&testOrder{
		orderNo: "U1", orderToken: "T-U1", userID: 1001,
		status: StatusPending, payMethod: "wechat",
		expireAt: time.Now().Add(-time.Minute),
	})

	mustNotErr(t, env.engine.CloseByUser(ctx, 1001, "U1"), "CloseByUser")
	mustEqual(t, env.store.byNo["U1"].status, StatusClosed, "final status")
}

func TestCloseByUser_WrongUserForbidden(t *testing.T) {
	env := newTestEnv(t)
	env.store.seed(&testOrder{
		orderNo: "U2", orderToken: "T-U2", userID: 1001,
		status: StatusPending, payMethod: "wechat",
		expireAt: time.Now().Add(-time.Minute),
	})

	err := env.engine.CloseByUser(context.Background(), 9999, "U2")
	if err == nil || err != ErrOrderForbidden {
		t.Fatalf("expected ErrOrderForbidden, got %v", err)
	}

	// 关闭不应发生
	mustEqual(t, env.store.byNo["U2"].status, StatusPending, "status unchanged")
	mustLen(t, env.OnClosedCalls, 0, "no close hook")
}

func TestCloseByUser_NotFound(t *testing.T) {
	env := newTestEnv(t)
	err := env.engine.CloseByUser(context.Background(), 1001, "NOPE")
	if err != ErrOrderNotFound {
		t.Fatalf("expected ErrOrderNotFound, got %v", err)
	}
}
