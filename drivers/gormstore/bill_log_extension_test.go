package gormstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gtkit/orderflow"
	"github.com/gtkit/orderflow/drivers/gormstore"
	"gorm.io/gorm"
)

// =============================================================================
// BillWriter / LogStore 接口替换 —— 自定义实现的覆盖
// =============================================================================

// fakeBillWriter 捕获 BillWriter.Write 的入参，便于断言。
type fakeBillWriter struct {
	calls    int
	lastSpec orderflow.BillSpec
	err      error // 非 nil 时 Write 返回该错误
}

func (w *fakeBillWriter) Write(_ *gorm.DB, spec orderflow.BillSpec) error {
	w.calls++
	w.lastSpec = spec
	return w.err
}

// fakeLogStore 捕获 Append / List 的入参与调用次数。
type fakeLogStore struct {
	appendCalls    int
	listCalls      int
	lastEntry      orderflow.LogEntry
	lastListOrder  string
	listResult     []orderflow.LogEntry
	appendErr      error
	listErr        error
}

func (s *fakeLogStore) Append(_ context.Context, _ *gorm.DB, entry orderflow.LogEntry) error {
	s.appendCalls++
	s.lastEntry = entry
	return s.appendErr
}

func (s *fakeLogStore) List(_ context.Context, _ *gorm.DB, orderNo string) ([]orderflow.LogEntry, error) {
	s.listCalls++
	s.lastListOrder = orderNo
	return s.listResult, s.listErr
}

// 自定义 BillWriter 替换默认实现：FinalizePaidOrder 走自定义路径，
// 订单状态推进到 Delivered，bills 表无新增行（自定义 writer 没真正写表）。
func TestStore_FinalizePaidOrder_CustomBillWriter(t *testing.T) {
	writer := &fakeBillWriter{}
	s, db := newTestStore(t, func(c *gormstore.Config[*orderRow, orderRow]) {
		c.BillWriter = writer
	})
	ctx := context.Background()

	seedOrder(t, s, &orderRow{
		OrderNoCol: "N", OrderTokenCol: "T", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPending, ExpireAtCol: time.Now().Add(time.Hour),
		PayAmountCol: 9900, OriginalPriceCol: 9900,
	})
	_, _ = s.CASConfirmPaid(ctx, "N", "TXN", time.Now())
	order, _, _ := s.GetByNo(ctx, "N")

	if err := s.FinalizePaidOrder(ctx, order, orderflow.BillSpec{
		UserID: 1, OrderNo: "N", PayAmount: 9900, PaidAt: time.Now(),
	}); err != nil {
		t.Fatalf("FinalizePaidOrder: %v", err)
	}

	if writer.calls != 1 {
		t.Errorf("BillWriter.Write calls = %d, want 1", writer.calls)
	}
	if writer.lastSpec.OrderNo != "N" || writer.lastSpec.PayAmount != 9900 {
		t.Errorf("BillWriter received unexpected spec: %+v", writer.lastSpec)
	}

	// 订单已推进到 Delivered
	got, _, _ := s.GetByNo(ctx, "N")
	if got.StatusCol != orderflow.StatusDelivered {
		t.Errorf("status = %v, want Delivered", got.StatusCol)
	}

	// 自定义 writer 没真正写 bills 表
	var count int64
	if err := db.Table("order_bills").Count(&count).Error; err != nil {
		t.Fatalf("count bills: %v", err)
	}
	if count != 0 {
		t.Errorf("bills table should be empty when custom writer doesn't persist, got %d", count)
	}
}

// 启用 ColumnMap.ChannelID + 自定义 BillWriter：driver 内部回查订单 channel_id，
// 合并到 BillSpec 副本后传给 writer，writer 应收到回查后的真实值。
func TestStore_FinalizePaidOrder_CustomBillWriter_ChannelIDPropagated(t *testing.T) {
	writer := &fakeBillWriter{}
	s, _ := newTestStore(t, func(c *gormstore.Config[*orderRow, orderRow]) {
		c.BillWriter = writer
		c.ColumnMap.ChannelID = "channel_id"
	})
	ctx := context.Background()

	seedOrder(t, s, &orderRow{
		OrderNoCol: "N", OrderTokenCol: "T", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPending, ExpireAtCol: time.Now().Add(time.Hour),
		PayAmountCol: 9900, OriginalPriceCol: 9900,
		ChannelIDCol: 42,
	})
	_, _ = s.CASConfirmPaid(ctx, "N", "TXN", time.Now())
	order, _, _ := s.GetByNo(ctx, "N")

	// BillSpec.ChannelID 故意传 0 触发 driver 回查
	if err := s.FinalizePaidOrder(ctx, order, orderflow.BillSpec{
		UserID: 1, OrderNo: "N", ChannelID: 0, PaidAt: time.Now(),
	}); err != nil {
		t.Fatalf("FinalizePaidOrder: %v", err)
	}
	if writer.lastSpec.ChannelID != 42 {
		t.Errorf("writer.lastSpec.ChannelID = %d, want 42 (driver should backfill from order row)",
			writer.lastSpec.ChannelID)
	}
}

// 自定义 BillWriter 返回 error 时整个事务回滚：订单状态保留 Paid。
func TestStore_FinalizePaidOrder_CustomBillWriter_ErrorRollsBack(t *testing.T) {
	writeErr := errors.New("custom writer failed: downstream unavailable")
	writer := &fakeBillWriter{err: writeErr}
	s, _ := newTestStore(t, func(c *gormstore.Config[*orderRow, orderRow]) {
		c.BillWriter = writer
	})
	ctx := context.Background()

	seedOrder(t, s, &orderRow{
		OrderNoCol: "N", OrderTokenCol: "T", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPending, ExpireAtCol: time.Now().Add(time.Hour),
		PayAmountCol: 9900, OriginalPriceCol: 9900,
	})
	_, _ = s.CASConfirmPaid(ctx, "N", "TXN", time.Now())
	order, _, _ := s.GetByNo(ctx, "N")

	err := s.FinalizePaidOrder(ctx, order, orderflow.BillSpec{
		UserID: 1, OrderNo: "N", PaidAt: time.Now(),
	})
	if !errors.Is(err, writeErr) {
		t.Fatalf("err = %v, want wraps %v", err, writeErr)
	}

	got, _, _ := s.GetByNo(ctx, "N")
	if got.StatusCol != orderflow.StatusPaid {
		t.Errorf("status = %v, want Paid (rolled back)", got.StatusCol)
	}
}

// 自定义 LogStore 替换默认实现：AppendLog / ListLogsByOrderNo 委托正确。
func TestStore_AppendLog_CustomLogStore(t *testing.T) {
	logStore := &fakeLogStore{
		listResult: []orderflow.LogEntry{
			{OrderNo: "N", FromStatus: orderflow.StatusPending, ToStatus: orderflow.StatusPaid},
		},
	}
	s, _ := newTestStore(t, func(c *gormstore.Config[*orderRow, orderRow]) {
		c.LogStore = logStore
	})
	ctx := context.Background()

	entry := orderflow.LogEntry{
		OrderNo: "N", UserID: 1,
		FromStatus: orderflow.StatusPending, ToStatus: orderflow.StatusPaid,
		Actor: "system", Remark: "paid by webhook",
	}
	if err := s.AppendLog(ctx, entry); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}
	if logStore.appendCalls != 1 {
		t.Errorf("LogStore.Append calls = %d, want 1", logStore.appendCalls)
	}
	if logStore.lastEntry.OrderNo != "N" || logStore.lastEntry.Remark != "paid by webhook" {
		t.Errorf("Append received unexpected entry: %+v", logStore.lastEntry)
	}

	// List 走自定义实现：返回值应直接传播
	entries, err := s.ListLogsByOrderNo(ctx, "N")
	if err != nil {
		t.Fatalf("ListLogsByOrderNo: %v", err)
	}
	if logStore.listCalls != 1 || logStore.lastListOrder != "N" {
		t.Errorf("LogStore.List not called with order N (calls=%d, last=%q)",
			logStore.listCalls, logStore.lastListOrder)
	}
	if len(entries) != 1 || entries[0].ToStatus != orderflow.StatusPaid {
		t.Errorf("entries propagation broken: %+v", entries)
	}
}

// 自定义 LogStore 返回的错误必须原样向上传播。
func TestStore_LogStore_ErrorsPropagate(t *testing.T) {
	appendErr := errors.New("append: db down")
	listErr := errors.New("list: query timeout")
	logStore := &fakeLogStore{appendErr: appendErr, listErr: listErr}
	s, _ := newTestStore(t, func(c *gormstore.Config[*orderRow, orderRow]) {
		c.LogStore = logStore
	})
	ctx := context.Background()

	if err := s.AppendLog(ctx, orderflow.LogEntry{OrderNo: "N"}); !errors.Is(err, appendErr) {
		t.Errorf("AppendLog err = %v, want %v", err, appendErr)
	}
	if _, err := s.ListLogsByOrderNo(ctx, "N"); !errors.Is(err, listErr) {
		t.Errorf("ListLogsByOrderNo err = %v, want %v", err, listErr)
	}
}
