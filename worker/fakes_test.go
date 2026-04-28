package worker

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gtkit/orderflow"
)

// Worker 测试需要一个真实 *orderflow.Engine[O]，所以补一组最小 fakes 注入到 Engine。
// 这些 fakes 的表面积显著小于 core 包里的完整版，只实现 worker 路径用到的方法和断言。

// ---- testOrder 最小 OrderSnapshot ----

type testOrder struct {
	orderNo    string
	orderToken string
	userID     int64
	status     orderflow.OrderStatus
	payMethod  string
	payAmount  int64
	tradeNo    string
	expireAt   time.Time
	paidAt     *time.Time
}

func (o *testOrder) OrderNo() string               { return o.orderNo }
func (o *testOrder) OrderToken() string            { return o.orderToken }
func (o *testOrder) UserID() int64                 { return o.userID }
func (o *testOrder) Status() orderflow.OrderStatus { return o.status }
func (o *testOrder) ProductID() uint64             { return 1 }
func (o *testOrder) ProductType() string           { return "" }
func (o *testOrder) ProductTitle() string          { return "T" }
func (o *testOrder) PayMethod() string             { return o.payMethod }
func (o *testOrder) PayAmount() int64              { return o.payAmount }
func (o *testOrder) OriginalPrice() int64          { return o.payAmount }
func (o *testOrder) TradeNo() string               { return o.tradeNo }
func (o *testOrder) ExpireAt() time.Time           { return o.expireAt }
func (o *testOrder) PaidAt() (time.Time, bool) {
	if o.paidAt == nil {
		return time.Time{}, false
	}
	return *o.paidAt, true
}
func (o *testOrder) Extra() map[string]any { return nil }

// ---- fakeStore ----

type fakeStore struct {
	mu    sync.Mutex
	byNo  map[string]*testOrder
	bills []orderflow.BillSpec
	logs  []orderflow.LogEntry

	CloseCalls         int
	ReconcileCompletes int
}

func newFakeStore() *fakeStore {
	return &fakeStore{byNo: make(map[string]*testOrder)}
}

func (s *fakeStore) seed(o *testOrder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byNo[o.orderNo] = o
}

func (s *fakeStore) GetByNo(_ context.Context, orderNo string, _ ...string) (*testOrder, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.byNo[orderNo]
	return o, ok, nil
}
func (s *fakeStore) GetByToken(_ context.Context, _ string) (*testOrder, bool, error) {
	return nil, false, nil
}
func (s *fakeStore) ListByUser(_ context.Context, _ int64, _ ...string) ([]*testOrder, error) {
	return nil, nil
}
func (s *fakeStore) FindPendingByUserAndProduct(_ context.Context, _ int64, _ uint64) (*testOrder, bool, error) {
	return nil, false, nil
}
func (s *fakeStore) FindExpiredPending(_ context.Context, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var out []string
	for _, o := range s.byNo {
		if o.status == orderflow.StatusPending && now.After(o.expireAt) {
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
		if o.status == orderflow.StatusPaid {
			out = append(out, o.orderNo)
		}
	}
	sort.Strings(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (s *fakeStore) Create(_ context.Context, _ orderflow.OrderSpec) (*testOrder, error) {
	return nil, nil
}
func (s *fakeStore) UpdateByOrderNo(_ context.Context, _ string, _ map[string]any) error {
	return nil
}
func (s *fakeStore) CASClose(_ context.Context, orderNo string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CloseCalls++
	o, ok := s.byNo[orderNo]
	if !ok || o.status != orderflow.StatusPending {
		return 0, nil
	}
	o.status = orderflow.StatusClosed
	return 1, nil
}
func (s *fakeStore) CASConfirmPaid(_ context.Context, _, _ string, _ time.Time) (int64, error) {
	return 0, nil
}
func (s *fakeStore) CASReopenPaid(_ context.Context, _, _ string, _ time.Time) (int64, error) {
	return 0, nil
}
func (s *fakeStore) FinalizePaidOrder(_ context.Context, order *testOrder, bill orderflow.BillSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ReconcileCompletes++
	if o, ok := s.byNo[order.orderNo]; ok {
		o.status = orderflow.StatusDelivered
	}
	s.bills = append(s.bills, bill)
	return nil
}
func (s *fakeStore) AppendLog(_ context.Context, entry orderflow.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = append(s.logs, entry)
	return nil
}
func (s *fakeStore) ListLogsByOrderNo(_ context.Context, _ string) ([]orderflow.LogEntry, error) {
	return nil, nil
}

// ---- 其他依赖：全 no-op 即可满足 worker 间接调用 ----

type fakeGateway struct{}

func (fakeGateway) UnifiedOrder(_ context.Context, _ orderflow.Channel, _ orderflow.UnifiedOrderRequest) (orderflow.UnifiedOrderResponse, error) {
	return orderflow.UnifiedOrderResponse{}, nil
}
func (fakeGateway) CloseOrder(_ context.Context, _ orderflow.Channel, _ string) error { return nil }
func (fakeGateway) QueryOrder(_ context.Context, _ orderflow.Channel, _ string) (orderflow.QueryResult, error) {
	return orderflow.QueryResult{}, nil
}
func (fakeGateway) ParseNotify(_ context.Context, _ orderflow.Channel, _ *http.Request) (orderflow.NotifyResult, error) {
	return orderflow.NotifyResult{}, nil
}
func (fakeGateway) AckNotify(_ orderflow.Channel, _ http.ResponseWriter) error { return nil }
func (fakeGateway) IsIgnorableCloseError(_ orderflow.Channel, _ error) bool    { return false }

// ---- DelayQueue ----

type fakeQueue struct {
	mu       sync.Mutex
	pending  map[string]time.Time // member -> executeAt
	acked    []string
	reserved []string
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{pending: make(map[string]time.Time)}
}

func (q *fakeQueue) Enqueue(_ context.Context, member string, executeAt time.Time) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending[member] = executeAt
	return true, nil
}
func (q *fakeQueue) Remove(_ context.Context, member string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.pending, member)
	return nil
}
func (q *fakeQueue) ReserveExpired(_ context.Context, batch int, _ time.Duration) ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	var ready []string
	for m, at := range q.pending {
		if now.After(at) || now.Equal(at) {
			ready = append(ready, m)
			delete(q.pending, m)
		}
		if len(ready) >= batch {
			break
		}
	}
	sort.Strings(ready)
	q.reserved = append(q.reserved, ready...)
	return ready, nil
}
func (q *fakeQueue) Ack(_ context.Context, member string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acked = append(q.acked, member)
	return true, nil
}
func (q *fakeQueue) RequeueExpired(_ context.Context, _ int) ([]string, error) {
	return nil, nil
}

// ---- Cache / Stream ----

type fakeCache struct{}

func (fakeCache) Set(_ context.Context, _ string, _ int64, _ orderflow.OrderStatus, _ time.Time) error {
	return nil
}
func (fakeCache) Get(_ context.Context, _ string) (orderflow.CachedStatus, bool, error) {
	return orderflow.CachedStatus{}, false, nil
}
func (fakeCache) Delete(_ context.Context, _ string) error { return nil }

type fakeStream struct{}

func (fakeStream) Publish(_ context.Context, _ string, _ orderflow.OrderStatus) error { return nil }
func (fakeStream) Subscribe(_ context.Context, _ string) (orderflow.Subscription, error) {
	return nil, nil
}

// ---- 组装：newTestEngine 返回一个真 Engine + 暴露内部 store/queue 供断言 ----

type testRig struct {
	engine *orderflow.Engine[*testOrder]
	store  *fakeStore
	queue  *fakeQueue
}

func newTestRig(t *testing.T) *testRig {
	t.Helper()
	store := newFakeStore()
	queue := newFakeQueue()
	eng, err := orderflow.New[*testOrder](orderflow.Config[*testOrder]{
		Store:      store,
		Gateway:    fakeGateway{},
		DelayQueue: queue,
		Cache:      fakeCache{},
		Stream:     fakeStream{},
		// Logger 留空，使用 orderflow 内置 nopLogger（丢弃所有日志）
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return &testRig{engine: eng, store: store, queue: queue}
}
