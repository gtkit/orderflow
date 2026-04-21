package worker

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderflow"
)

// Stats 的语义验证：
//   - 空 poll 计入 PollsTotal，Inflight 归零，LastError 为空；
//   - 有任务的 poll 记录 LastBatchSize；
//   - 错误注入时 PollErrors 递增 + LastError 非空。

func TestCloseWorker_StatsInitialState(t *testing.T) {
	rig := newTestRig(t)
	w := NewCloseWorker(rig.engine, CloseOptions{})

	s := w.Stats()
	mustEqual(t, s.Inflight, int64(0), "initial Inflight")
	mustEqual(t, s.PollsTotal, int64(0), "initial PollsTotal")
	mustEqual(t, s.PollErrors, int64(0), "initial PollErrors")
	if !s.LastPollAt.IsZero() {
		t.Errorf("initial LastPollAt should be zero, got %v", s.LastPollAt)
	}
}

func TestCloseWorker_StatsAfterEmptyPoll(t *testing.T) {
	rig := newTestRig(t)
	w := NewCloseWorker(rig.engine, CloseOptions{})

	_ = w.poll(context.Background())
	w.wg.Wait()

	s := w.Stats()
	mustEqual(t, s.PollsTotal, int64(1), "PollsTotal")
	mustEqual(t, s.PollErrors, int64(0), "no errors")
	mustEqual(t, s.LastBatchSize, int64(0), "empty batch")
	mustEqual(t, s.Inflight, int64(0), "inflight after drain")
	if s.LastPollAt.IsZero() {
		t.Error("LastPollAt should be set")
	}
	if s.LastError != "" {
		t.Errorf("LastError = %q, want empty", s.LastError)
	}
}

func TestCloseWorker_StatsRecordsBatchSize(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	now := time.Now()

	for _, n := range []string{"A", "B", "C", "D"} {
		rig.store.seed(&testOrder{
			orderNo: n, orderToken: "t-" + n,
			status:    orderflow.StatusPending,
			payMethod: "wechat",
			expireAt:  now.Add(-time.Minute),
		})
		_, _ = rig.queue.Enqueue(ctx, n, now.Add(-time.Minute))
	}

	w := NewCloseWorker(rig.engine, CloseOptions{PollBatchSize: 10, MaxWorkers: 4})
	_ = w.poll(ctx)
	w.wg.Wait()

	s := w.Stats()
	mustEqual(t, s.PollsTotal, int64(1), "PollsTotal")
	mustEqual(t, s.LastBatchSize, int64(4), "batch size reflects 4 expired")
	mustEqual(t, s.Inflight, int64(0), "inflight after drain")
}

// 使用故障注入：让 queue.ReserveExpired 失败，验证 PollErrors + LastError 记录。
type failingQueue struct {
	*fakeQueue
	err error
}

func (q *failingQueue) ReserveExpired(_ context.Context, _ int, _ time.Duration) ([]string, error) {
	return nil, q.err
}

func TestCloseWorker_StatsRecordsError(t *testing.T) {
	_ = failingQueue{} // 占位确保类型被使用
	// 直接构造 rig 并替换 engine 的 delayQueue 太复杂，改用间接测试路径：
	// 让 queue 里的 Requeue 先失败就能观察到错误——但 requeue 失败只是警告不中断 poll。
	// 为保持测试最小可复现，用 ReserveExpired 失败路径：在 fakeQueue 上加错误注入字段。
	// 见下面 TestCloseWorker_RequeueFailureCountedInStats。
	t.Skip("covered by TestCloseWorker_ReserveFailureCountedInStats below")
}

// 扩展 fakeQueue 的错误注入：ReserveExpiredErr 让 queue 在 Reserve 时失败。
// 这里复用现有 fakeQueue 结构并包装。
type reserveFailingQueue struct {
	*fakeQueue
	err error
}

func (q *reserveFailingQueue) ReserveExpired(_ context.Context, _ int, _ time.Duration) ([]string, error) {
	return nil, q.err
}

func TestCloseWorker_ReserveFailureCountedInStats(t *testing.T) {
	rig := newTestRig(t)
	// 包装 queue 让 ReserveExpired 失败
	wrap := &reserveFailingQueue{fakeQueue: rig.queue, err: errTestReserveFail}

	// 直接构造新 Engine 注入 wrap queue
	eng, err := orderflow.New[*testOrder](orderflow.Config[*testOrder]{
		Store:      rig.store,
		Gateway:    fakeGateway{},
		DelayQueue: wrap,
		Cache:      fakeCache{},
		Stream:     fakeStream{},
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	w := NewCloseWorker(eng, CloseOptions{})
	_ = w.poll(context.Background())

	s := w.Stats()
	mustEqual(t, s.PollErrors, int64(1), "PollErrors")
	if s.LastError == "" {
		t.Error("LastError should be non-empty")
	}
}

var errTestReserveFail = &testErr{msg: "reserve failed"}

type testErr struct{ msg string }

func (e *testErr) Error() string { return e.msg }
