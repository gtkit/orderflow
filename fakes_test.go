package orderflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// testOrder —— 测试用的 OrderSnapshot 实现
// =============================================================================

type testOrder struct {
	orderNo       string
	orderToken    string
	userID        int64
	status        OrderStatus
	productID     uint64
	productType   string
	productTitle  string
	payMethod     string
	payAmount     int64
	originalPrice int64
	tradeNo       string
	expireAt      time.Time
	paidAt        *time.Time
	channelID     int64
	extra         map[string]any
}

func (o *testOrder) OrderNo() string      { return o.orderNo }
func (o *testOrder) OrderToken() string   { return o.orderToken }
func (o *testOrder) UserID() int64        { return o.userID }
func (o *testOrder) Status() OrderStatus  { return o.status }
func (o *testOrder) ProductID() uint64    { return o.productID }
func (o *testOrder) ProductType() string  { return o.productType }
func (o *testOrder) ProductTitle() string { return o.productTitle }
func (o *testOrder) PayMethod() string    { return o.payMethod }
func (o *testOrder) PayAmount() int64     { return o.payAmount }
func (o *testOrder) OriginalPrice() int64 { return o.originalPrice }
func (o *testOrder) TradeNo() string      { return o.tradeNo }
func (o *testOrder) ExpireAt() time.Time  { return o.expireAt }
func (o *testOrder) PaidAt() (time.Time, bool) {
	if o.paidAt == nil {
		return time.Time{}, false
	}
	return *o.paidAt, true
}
func (o *testOrder) Extra() map[string]any { return o.extra }

// =============================================================================
// fakeStore —— 内存 Store[*testOrder]，带错误注入与调用计数
// =============================================================================

type fakeStore struct {
	mu      sync.Mutex
	byNo    map[string]*testOrder
	byToken map[string]*testOrder
	bills   []BillSpec
	logs    []LogEntry

	// Error injection
	ErrOnCreate    error
	ErrOnGet       error
	ErrOnCAS       error
	ErrOnFinalize  error
	ErrOnAppendLog error

	// Call counters
	CreateCalls         int
	CASCloseCalls       int
	CASConfirmPaidCalls int
	CASReopenPaidCalls  int
	FinalizeCalls       int
	AppendLogCalls      int

	// Race injection：CASConfirmPaid 第一次返回 0，模拟并发抢跑。
	ConfirmPaidRaceOnce bool

	// Race injection：CASClose 第一次返回 0 且把订单改成 Paid（+ 交易号），
	// 模拟"关闭 CAS 失败，因为支付回调抢先推进了状态"的竞态。
	CASCloseLosesToPaidOnce bool

	// Race injection：CASReopenPaid 第一次返回 0（affected=0），
	// 模拟"已 Closed 订单在 CAS Reopen 之前被其他并发操作改成了非 Closed"的竞态——
	// 这种 miss 应该被 engine 静默处理（log info），不上报 anomaly。
	CASReopenMissOnce bool

	// Race injection：CASConfirmPaid 第一次返回 0 并把订单状态改为指定值。
	// 比 ConfirmPaidRaceOnce 更灵活——可以测试 recheck 看到 Closed / Delivered / Cancelled
	// 等不同分支。零值（StatusUnknown）不启用。
	ConfirmPaidRaceToStatus OrderStatus

	// Race injection：CASConfirmPaid 第一次返回 0 并从 byNo 删除订单，
	// 模拟"订单在 CAS 期间被并发操作清理"（虽然 engine 不会这么做，但作为防御性测试）。
	ConfirmPaidMakeDisappearOnce bool

	// PostCreate 允许测试在 Create 完成后再调整 order（模拟并发修改）
	PostCreate func(*testOrder)
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		byNo:    make(map[string]*testOrder),
		byToken: make(map[string]*testOrder),
	}
}

func (s *fakeStore) seed(o *testOrder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byNo[o.orderNo] = o
	s.byToken[o.orderToken] = o
}

// snapshot 返回订单的深拷贝——仿照真 DB 的 SELECT 返回独立行语义。
// 避免 fakeStore 把内部可变指针泄漏给 Engine，导致并发场景下 Status() 等 getter 读取
// 与 CAS 方法的字段写入产生 race。所有对外返回 *testOrder 的方法都必须通过此 helper。
func (s *fakeStore) snapshot(o *testOrder) *testOrder {
	if o == nil {
		return nil
	}
	cp := *o
	return &cp
}

func (s *fakeStore) GetByNo(_ context.Context, orderNo string, _ ...string) (*testOrder, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ErrOnGet != nil {
		return nil, false, s.ErrOnGet
	}
	o, ok := s.byNo[orderNo]
	return s.snapshot(o), ok, nil
}

func (s *fakeStore) GetByToken(_ context.Context, orderToken string) (*testOrder, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ErrOnGet != nil {
		return nil, false, s.ErrOnGet
	}
	o, ok := s.byToken[orderToken]
	return s.snapshot(o), ok, nil
}

func (s *fakeStore) ListByUser(_ context.Context, userID int64, _ ...string) ([]*testOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*testOrder
	for _, o := range s.byNo {
		if o.userID == userID {
			out = append(out, s.snapshot(o))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].orderNo < out[j].orderNo })
	return out, nil
}

func (s *fakeStore) FindPendingByUserAndProduct(_ context.Context, userID int64, productID uint64) (*testOrder, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.byNo {
		if o.userID == userID && o.productID == productID && o.status == StatusPending {
			return s.snapshot(o), true, nil
		}
	}
	return nil, false, nil
}

func (s *fakeStore) FindExpiredPending(_ context.Context, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var out []string
	for _, o := range s.byNo {
		if o.status == StatusPending && now.After(o.expireAt) {
			out = append(out, o.orderNo)
		}
	}
	sort.Strings(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *fakeStore) FindPaidUndelivered(_ context.Context, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, o := range s.byNo {
		if o.status == StatusPaid {
			out = append(out, o.orderNo)
		}
	}
	sort.Strings(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *fakeStore) Create(_ context.Context, spec OrderSpec) (*testOrder, error) {
	s.mu.Lock()
	s.CreateCalls++
	if s.ErrOnCreate != nil {
		err := s.ErrOnCreate
		s.mu.Unlock()
		return nil, err
	}
	o := &testOrder{
		orderNo:       spec.OrderNo,
		orderToken:    spec.OrderToken,
		userID:        spec.UserID,
		status:        spec.Status,
		productID:     spec.ProductID,
		productType:   spec.ProductType,
		productTitle:  spec.ProductTitle,
		originalPrice: spec.OriginalPrice,
		payAmount:     spec.PayAmount,
		payMethod:     spec.PayMethod,
		channelID:     spec.ChannelID,
		expireAt:      spec.ExpireAt,
		extra:         spec.Extra,
	}
	s.byNo[o.orderNo] = o
	s.byToken[o.orderToken] = o
	snap := s.snapshot(o)
	post := s.PostCreate
	s.mu.Unlock()
	if post != nil {
		post(o)
	}
	return snap, nil
}

func (s *fakeStore) UpdateByOrderNo(_ context.Context, _ string, _ map[string]any) error {
	return nil
}

func (s *fakeStore) CASClose(_ context.Context, orderNo string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CASCloseCalls++
	if s.ErrOnCAS != nil {
		return 0, s.ErrOnCAS
	}
	if s.CASCloseLosesToPaidOnce {
		s.CASCloseLosesToPaidOnce = false
		if o, ok := s.byNo[orderNo]; ok {
			o.status = StatusPaid
			o.tradeNo = "TXN-RACE"
			now := time.Now()
			o.paidAt = &now
		}
		return 0, nil
	}
	o, ok := s.byNo[orderNo]
	if !ok || o.status != StatusPending {
		return 0, nil
	}
	o.status = StatusClosed
	return 1, nil
}

func (s *fakeStore) CASConfirmPaid(_ context.Context, orderNo, tradeNo string, paidAt time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CASConfirmPaidCalls++
	if s.ErrOnCAS != nil {
		return 0, s.ErrOnCAS
	}
	if s.ConfirmPaidRaceOnce {
		s.ConfirmPaidRaceOnce = false
		if o, ok := s.byNo[orderNo]; ok {
			// 模拟并发：某个并行路径已把 Pending 推进到 Paid，
			// 所以我们的 CAS 在 WHERE status=Pending 上匹配不到行。
			o.status = StatusPaid
			o.tradeNo = tradeNo
			cp := paidAt
			o.paidAt = &cp
		}
		return 0, nil
	}
	if s.ConfirmPaidRaceToStatus != 0 {
		target := s.ConfirmPaidRaceToStatus
		s.ConfirmPaidRaceToStatus = 0
		if o, ok := s.byNo[orderNo]; ok {
			o.status = target
		}
		return 0, nil
	}
	if s.ConfirmPaidMakeDisappearOnce {
		s.ConfirmPaidMakeDisappearOnce = false
		delete(s.byNo, orderNo)
		return 0, nil
	}
	o, ok := s.byNo[orderNo]
	if !ok || o.status != StatusPending {
		return 0, nil
	}
	o.status = StatusPaid
	o.tradeNo = tradeNo
	cp := paidAt
	o.paidAt = &cp
	return 1, nil
}

func (s *fakeStore) CASReopenPaid(_ context.Context, orderNo, tradeNo string, paidAt time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CASReopenPaidCalls++
	if s.ErrOnCAS != nil {
		return 0, s.ErrOnCAS
	}
	if s.CASReopenMissOnce {
		s.CASReopenMissOnce = false
		// 模拟并发：其他 goroutine 已经把这个 Closed 订单推进成 Paid（或 Delivered），
		// 我们的 WHERE status=Closed 不再命中。
		return 0, nil
	}
	o, ok := s.byNo[orderNo]
	if !ok || o.status != StatusClosed {
		return 0, nil
	}
	o.status = StatusPaid
	o.tradeNo = tradeNo
	cp := paidAt
	o.paidAt = &cp
	return 1, nil
}

func (s *fakeStore) FinalizePaidOrder(_ context.Context, order *testOrder, bill BillSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.FinalizeCalls++
	if s.ErrOnFinalize != nil {
		return s.ErrOnFinalize
	}
	o, ok := s.byNo[order.orderNo]
	if !ok {
		return errors.New("fakeStore: order not found on finalize")
	}
	if o.status != StatusPaid {
		return fmt.Errorf("fakeStore: finalize requires Paid, got %s", o.status)
	}
	o.status = StatusDelivered
	s.bills = append(s.bills, bill)
	return nil
}

func (s *fakeStore) AppendLog(_ context.Context, entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.AppendLogCalls++
	if s.ErrOnAppendLog != nil {
		return s.ErrOnAppendLog
	}
	s.logs = append(s.logs, entry)
	return nil
}

func (s *fakeStore) ListLogsByOrderNo(_ context.Context, orderNo string) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []LogEntry
	for _, l := range s.logs {
		if l.OrderNo == orderNo {
			out = append(out, l)
		}
	}
	return out, nil
}

// =============================================================================
// fakeGateway —— 可编程的 PaymentGateway
// =============================================================================

type fakeGateway struct {
	mu sync.Mutex

	UnifiedOrderResp UnifiedOrderResponse
	UnifiedOrderErr  error
	CloseOrderErr    error
	QueryResp        QueryResult
	QueryErr         error
	NotifyResult     NotifyResult
	ParseNotifyErr   error
	IgnorableFn      func(ch Channel, err error) bool

	UnifiedOrderCalls int
	CloseOrderCalls   int
	QueryOrderCalls   int
	ParseNotifyCalls  int
	AckNotifyCalls    int
}

func newFakeGateway() *fakeGateway {
	return &fakeGateway{
		UnifiedOrderResp: UnifiedOrderResponse{AppParams: "mock_pay_params"},
	}
}

func (g *fakeGateway) UnifiedOrder(_ context.Context, _ Channel, _ UnifiedOrderRequest) (UnifiedOrderResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.UnifiedOrderCalls++
	if g.UnifiedOrderErr != nil {
		return UnifiedOrderResponse{}, g.UnifiedOrderErr
	}
	return g.UnifiedOrderResp, nil
}

func (g *fakeGateway) CloseOrder(_ context.Context, _ Channel, _ string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.CloseOrderCalls++
	return g.CloseOrderErr
}

func (g *fakeGateway) QueryOrder(_ context.Context, _ Channel, _ string) (QueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.QueryOrderCalls++
	if g.QueryErr != nil {
		return QueryResult{}, g.QueryErr
	}
	return g.QueryResp, nil
}

func (g *fakeGateway) ParseNotify(_ context.Context, ch Channel, _ *http.Request) (NotifyResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ParseNotifyCalls++
	if g.ParseNotifyErr != nil {
		return NotifyResult{}, g.ParseNotifyErr
	}
	res := g.NotifyResult
	if res.Channel == "" {
		res.Channel = ch
	}
	return res, nil
}

func (g *fakeGateway) AckNotify(_ Channel, _ http.ResponseWriter) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.AckNotifyCalls++
	return nil
}

func (g *fakeGateway) IsIgnorableCloseError(ch Channel, err error) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.IgnorableFn != nil {
		return g.IgnorableFn(ch, err)
	}
	return false
}

// =============================================================================
// fakeDelayQueue —— 内存延时队列
// =============================================================================

type fakeDelayQueue struct {
	mu       sync.Mutex
	enqueued map[string]time.Time
	removed  []string
	reserved []string

	ErrOnEnqueue error
	ErrOnRemove  error

	EnqueueCalls int
	RemoveCalls  int
}

func newFakeDelayQueue() *fakeDelayQueue {
	return &fakeDelayQueue{
		enqueued: make(map[string]time.Time),
	}
}

func (q *fakeDelayQueue) Enqueue(_ context.Context, member string, executeAt time.Time) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.EnqueueCalls++
	if q.ErrOnEnqueue != nil {
		return false, q.ErrOnEnqueue
	}
	if _, exists := q.enqueued[member]; exists {
		return false, nil
	}
	q.enqueued[member] = executeAt
	return true, nil
}

func (q *fakeDelayQueue) Remove(_ context.Context, member string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.RemoveCalls++
	if q.ErrOnRemove != nil {
		return q.ErrOnRemove
	}
	delete(q.enqueued, member)
	q.removed = append(q.removed, member)
	return nil
}

func (q *fakeDelayQueue) ReserveExpired(_ context.Context, batch int, _ time.Duration) ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	var out []string
	for m, at := range q.enqueued {
		if now.After(at) || now.Equal(at) {
			out = append(out, m)
			delete(q.enqueued, m)
			if len(out) >= batch {
				break
			}
		}
	}
	sort.Strings(out)
	q.reserved = append(q.reserved, out...)
	return out, nil
}

func (q *fakeDelayQueue) Ack(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func (q *fakeDelayQueue) RequeueExpired(_ context.Context, _ int) ([]string, error) {
	return nil, nil
}

// =============================================================================
// fakeCache —— 内存 StatusCache
// =============================================================================

type fakeCacheEntry struct {
	userID int64
	status OrderStatus
}

type fakeCache struct {
	mu      sync.Mutex
	entries map[string]fakeCacheEntry

	ErrOnSet error

	SetCalls    int
	GetCalls    int
	DeleteCalls int

	// Ordered history of Set calls, for state-transition verification
	SetHistory []setEvent
}

type setEvent struct {
	OrderToken string
	UserID     int64
	Status     OrderStatus
}

func newFakeCache() *fakeCache {
	return &fakeCache{entries: make(map[string]fakeCacheEntry)}
}

func (c *fakeCache) Set(_ context.Context, orderToken string, userID int64, status OrderStatus, _ time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.SetCalls++
	c.SetHistory = append(c.SetHistory, setEvent{orderToken, userID, status})
	if c.ErrOnSet != nil {
		return c.ErrOnSet
	}
	c.entries[orderToken] = fakeCacheEntry{userID, status}
	return nil
}

func (c *fakeCache) Get(_ context.Context, orderToken string) (CachedStatus, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.GetCalls++
	e, ok := c.entries[orderToken]
	if !ok {
		return CachedStatus{}, false, nil
	}
	return CachedStatus{UserID: e.userID, Status: e.status}, true, nil
}

func (c *fakeCache) Delete(_ context.Context, orderToken string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.DeleteCalls++
	delete(c.entries, orderToken)
	return nil
}

// =============================================================================
// fakeStream —— 内存 StatusStream
// =============================================================================

type publishEvent struct {
	OrderToken string
	Status     OrderStatus
}

type fakeStream struct {
	mu        sync.Mutex
	Published []publishEvent
}

func newFakeStream() *fakeStream {
	return &fakeStream{}
}

func (s *fakeStream) Publish(_ context.Context, orderToken string, status OrderStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Published = append(s.Published, publishEvent{orderToken, status})
	return nil
}

func (s *fakeStream) Subscribe(_ context.Context, _ string) (Subscription, error) {
	return &fakeSubscription{events: make(chan OrderStatus)}, nil
}

type fakeSubscription struct {
	events chan OrderStatus
	once   sync.Once
}

func (s *fakeSubscription) Events() <-chan OrderStatus { return s.events }
func (s *fakeSubscription) Close() error {
	s.once.Do(func() { close(s.events) })
	return nil
}

// =============================================================================
// testEnv —— 组装 Engine + 全部 fakes，追踪所有钩子调用
// =============================================================================

type onPaidCall struct {
	OrderNo string
	TradeNo string
}

type onClosedCall struct {
	OrderNo string
	Reason  ClosedReason
}

type onSupersededCall struct {
	OldOrderNo   string
	NewProductID uint64
}

type onAnomalyCall struct {
	OrderNo string
	Kind    AnomalyKind
	Detail  string
}

type testEnv struct {
	t      *testing.T
	engine *Engine[*testOrder]

	store  *fakeStore
	gw     *fakeGateway
	dq     *fakeDelayQueue
	cache  *fakeCache
	stream *fakeStream

	mu                sync.Mutex
	OnCreatedCalls    []string
	OnPaidCalls       []onPaidCall
	OnDeliveredCalls  []string
	OnClosedCalls     []onClosedCall
	OnReopenedCalls   []string
	OnSupersededCalls []onSupersededCall
	OnAnomalyCalls    []onAnomalyCall

	// Hook behavior injection
	OnPaidErr error
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	env := &testEnv{
		t:      t,
		store:  newFakeStore(),
		gw:     newFakeGateway(),
		dq:     newFakeDelayQueue(),
		cache:  newFakeCache(),
		stream: newFakeStream(),
	}

	cfg := Config[*testOrder]{
		Store:      env.store,
		Gateway:    env.gw,
		DelayQueue: env.dq,
		Cache:      env.cache,
		Stream:     env.stream,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),

		OnCreated: func(_ context.Context, o *testOrder) error {
			env.mu.Lock()
			env.OnCreatedCalls = append(env.OnCreatedCalls, o.OrderNo())
			env.mu.Unlock()
			return nil
		},
		OnPaid: func(_ context.Context, o *testOrder, n NotifyResult) error {
			env.mu.Lock()
			env.OnPaidCalls = append(env.OnPaidCalls, onPaidCall{o.OrderNo(), n.TransactionID})
			err := env.OnPaidErr
			env.mu.Unlock()
			return err
		},
		OnDelivered: func(_ context.Context, o *testOrder) error {
			env.mu.Lock()
			env.OnDeliveredCalls = append(env.OnDeliveredCalls, o.OrderNo())
			env.mu.Unlock()
			return nil
		},
		OnClosed: func(_ context.Context, o *testOrder, reason ClosedReason) {
			env.mu.Lock()
			env.OnClosedCalls = append(env.OnClosedCalls, onClosedCall{o.OrderNo(), reason})
			env.mu.Unlock()
		},
		OnReopened: func(_ context.Context, o *testOrder, _ NotifyResult) {
			env.mu.Lock()
			env.OnReopenedCalls = append(env.OnReopenedCalls, o.OrderNo())
			env.mu.Unlock()
		},
		OnSuperseded: func(_ context.Context, o *testOrder, newProductID uint64) {
			env.mu.Lock()
			env.OnSupersededCalls = append(env.OnSupersededCalls, onSupersededCall{o.OrderNo(), newProductID})
			env.mu.Unlock()
		},
		OnAnomaly: func(_ context.Context, o *testOrder, kind AnomalyKind, detail string) {
			env.mu.Lock()
			env.OnAnomalyCalls = append(env.OnAnomalyCalls, onAnomalyCall{o.OrderNo(), kind, detail})
			env.mu.Unlock()
		},
	}
	eng, err := New[*testOrder](cfg)
	if err != nil {
		t.Fatalf("newTestEnv: %v", err)
	}
	env.engine = eng
	return env
}

// standardRequest 构造通用 CreateRequest，便于多数测试复用
func standardRequest() CreateRequest {
	return CreateRequest{
		UserID:    1001,
		PayMethod: "wechat",
		ChannelID: 1,
		ClientIP:  "127.0.0.1",
		Product: ProductInfo{
			ID:    2001,
			Type:  "membership",
			Title: "VIP 年卡",
			Price: 9900,
			Extra: map[string]any{"vip_type": int8(2), "vip_days": int64(365)},
		},
	}
}

// makeHTTPNotifyRequest 构造空的 *http.Request 供 HandleNotify 测试使用。
// fakeGateway.ParseNotify 忽略请求内容，只返回预置的 NotifyResult。
func makeHTTPNotifyRequest() *http.Request {
	return httptest.NewRequest(http.MethodPost, "/notify", bytes.NewReader(nil))
}

// =============================================================================
// 断言辅助
// =============================================================================

func mustNotErr(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", msg, err)
	}
}

func mustErr(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error, got nil", msg)
	}
}

func mustEqual[T comparable](t *testing.T, got, want T, msg string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", msg, got, want)
	}
}

func mustLen[T any](t *testing.T, got []T, want int, msg string) {
	t.Helper()
	if len(got) != want {
		t.Fatalf("%s: len=%d, want %d (got=%v)", msg, len(got), want, got)
	}
}

// assert 仅断言计数上限，超出即失败
func mustLTE(t *testing.T, got, max int, msg string) {
	t.Helper()
	if got > max {
		t.Fatalf("%s: got %d, want <= %d", msg, got, max)
	}
}
