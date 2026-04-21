package gormstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/gtkit/orderflow"
	"github.com/gtkit/orderflow/drivers/gormstore"
	"gorm.io/gorm"
)

// =============================================================================
// orderRow：测试模型，同时承担 GORM 模型 (M) 与 OrderSnapshot (O) 两个角色
// =============================================================================
//
// 字段命名用 Col 后缀避免与 OrderSnapshot 接口方法冲突。这是纯粹的测试 artefact，
// 业务项目通常有独立的 Order 结构体 + OrderView 包装器，两步适配更清晰。

type orderRow struct {
	ID               uint64                `gorm:"primaryKey;autoIncrement"`
	OrderNoCol       string                `gorm:"column:order_no;uniqueIndex;size:64;not null"`
	OrderTokenCol    string                `gorm:"column:order_token;uniqueIndex;size:64;not null"`
	UserIDCol        int64                 `gorm:"column:user_id;not null;index"`
	StatusCol        orderflow.OrderStatus `gorm:"column:status;not null"`
	ProductIDCol     uint64                `gorm:"column:product_id;not null"`
	ProductTypeCol   string                `gorm:"column:product_type;size:32;not null;default:''"`
	ProductTitleCol  string                `gorm:"column:product_title;size:255;not null"`
	PayMethodCol     string                `gorm:"column:pay_method;size:32;not null;default:''"`
	PayAmountCol     int64                 `gorm:"column:pay_amount;not null"`
	OriginalPriceCol int64                 `gorm:"column:original_price;not null"`
	TradeNoCol       string                `gorm:"column:trade_no;size:128;not null;default:''"`
	ExpireAtCol      time.Time             `gorm:"column:expire_at;not null"`
	PaidAtCol        *time.Time            `gorm:"column:paid_at"`
	DeliveredAtCol   *time.Time            `gorm:"column:delivered_at"`
	UpdatedAtCol     time.Time             `gorm:"column:updated_at"`
	ChannelIDCol     int64                 `gorm:"column:channel_id;not null;default:0"`
}

func (orderRow) TableName() string { return "orders_test" }

func (o *orderRow) OrderNo() string               { return o.OrderNoCol }
func (o *orderRow) OrderToken() string            { return o.OrderTokenCol }
func (o *orderRow) UserID() int64                 { return o.UserIDCol }
func (o *orderRow) Status() orderflow.OrderStatus { return o.StatusCol }
func (o *orderRow) ProductID() uint64             { return o.ProductIDCol }
func (o *orderRow) ProductType() string           { return o.ProductTypeCol }
func (o *orderRow) ProductTitle() string          { return o.ProductTitleCol }
func (o *orderRow) PayMethod() string             { return o.PayMethodCol }
func (o *orderRow) PayAmount() int64              { return o.PayAmountCol }
func (o *orderRow) OriginalPrice() int64          { return o.OriginalPriceCol }
func (o *orderRow) TradeNo() string               { return o.TradeNoCol }
func (o *orderRow) ExpireAt() time.Time           { return o.ExpireAtCol }
func (o *orderRow) PaidAt() (time.Time, bool) {
	if o.PaidAtCol == nil {
		return time.Time{}, false
	}
	return *o.PaidAtCol, true
}
func (o *orderRow) Extra() map[string]any { return nil }

// billModel / logModel 的表名在 Config 里指定；结构字段名不参与测试，GORM 按默认规则推断。

// =============================================================================
// 测试辅助
// =============================================================================

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:gormstore_%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&orderRow{}, &gormstore.OrderBill{}, &gormstore.OrderLog{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

func newTestStore(t *testing.T, opts ...func(*gormstore.Config[*orderRow, orderRow])) (*gormstore.Store[*orderRow, orderRow], *gorm.DB) {
	t.Helper()
	db := newTestDB(t)

	cfg := gormstore.Config[*orderRow, orderRow]{
		DB:         db,
		OrderTable: "orders_test",
		BillTable:  "order_bills",
		LogTable:   "order_logs",
		Wrap:       func(m *orderRow) *orderRow { return m },
		BuildModel: func(spec orderflow.OrderSpec) *orderRow {
			return &orderRow{
				OrderNoCol:       spec.OrderNo,
				OrderTokenCol:    spec.OrderToken,
				UserIDCol:        spec.UserID,
				StatusCol:        spec.Status,
				ProductIDCol:     spec.ProductID,
				ProductTypeCol:   spec.ProductType,
				ProductTitleCol:  spec.ProductTitle,
				PayAmountCol:     spec.PayAmount,
				OriginalPriceCol: spec.OriginalPrice,
				PayMethodCol:     spec.PayMethod,
				ChannelIDCol:     spec.ChannelID,
				ExpireAtCol:      spec.ExpireAt,
				UpdatedAtCol:     time.Now(),
			}
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	s, err := gormstore.New[*orderRow, orderRow](cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, db
}

func seedOrder(t *testing.T, s *gormstore.Store[*orderRow, orderRow], o *orderRow) *orderRow {
	t.Helper()
	ctx := context.Background()
	spec := orderflow.OrderSpec{
		OrderNo:       o.OrderNoCol,
		OrderToken:    o.OrderTokenCol,
		UserID:        o.UserIDCol,
		Status:        o.StatusCol,
		ProductID:     o.ProductIDCol,
		ProductType:   o.ProductTypeCol,
		ProductTitle:  o.ProductTitleCol,
		PayAmount:     o.PayAmountCol,
		OriginalPrice: o.OriginalPriceCol,
		PayMethod:     o.PayMethodCol,
		ChannelID:     o.ChannelIDCol,
		ExpireAt:      o.ExpireAtCol,
	}
	got, err := s.Create(ctx, spec)
	if err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	return got
}

// =============================================================================
// 构造器与校验
// =============================================================================

func TestNew_RejectsMissingFields(t *testing.T) {
	db := newTestDB(t)
	baseCfg := gormstore.Config[*orderRow, orderRow]{
		DB:         db,
		OrderTable: "orders_test",
		BillTable:  "order_bills",
		LogTable:   "order_logs",
		Wrap:       func(m *orderRow) *orderRow { return m },
		BuildModel: func(s orderflow.OrderSpec) *orderRow { return &orderRow{} },
	}
	if _, err := gormstore.New[*orderRow, orderRow](baseCfg); err != nil {
		t.Fatalf("baseline: %v", err)
	}

	cases := []struct {
		name    string
		mutator func(*gormstore.Config[*orderRow, orderRow])
	}{
		{"DB nil", func(c *gormstore.Config[*orderRow, orderRow]) { c.DB = nil }},
		{"OrderTable empty", func(c *gormstore.Config[*orderRow, orderRow]) { c.OrderTable = "" }},
		{"BillTable empty", func(c *gormstore.Config[*orderRow, orderRow]) { c.BillTable = "" }},
		{"LogTable empty", func(c *gormstore.Config[*orderRow, orderRow]) { c.LogTable = "" }},
		{"Wrap nil", func(c *gormstore.Config[*orderRow, orderRow]) { c.Wrap = nil }},
		{"BuildModel nil", func(c *gormstore.Config[*orderRow, orderRow]) { c.BuildModel = nil }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := baseCfg
			c.mutator(&cfg)
			if _, err := gormstore.New[*orderRow, orderRow](cfg); err == nil {
				t.Fatalf("expected validation error for %s", c.name)
			}
		})
	}
}

func TestNew_RejectsMaliciousColumnNames(t *testing.T) {
	db := newTestDB(t)
	baseCfg := gormstore.Config[*orderRow, orderRow]{
		DB:         db,
		OrderTable: "orders_test",
		BillTable:  "order_bills",
		LogTable:   "order_logs",
		Wrap:       func(m *orderRow) *orderRow { return m },
		BuildModel: func(s orderflow.OrderSpec) *orderRow { return &orderRow{} },
	}

	cases := []struct {
		name string
		cm   gormstore.ColumnMap
	}{
		{"SQL injection attempt", gormstore.ColumnMap{Status: "status = 1 OR 1=1 --"}},
		{"whitespace", gormstore.ColumnMap{OrderNo: "order no"}},
		{"starts with digit", gormstore.ColumnMap{UserID: "1user_id"}},
		{"contains quote", gormstore.ColumnMap{TradeNo: "trade_no'"}},
		{"contains semicolon", gormstore.ColumnMap{ExpireAt: "expire_at; DROP TABLE orders"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := baseCfg
			cfg.ColumnMap = c.cm
			if _, err := gormstore.New[*orderRow, orderRow](cfg); err == nil {
				t.Fatalf("expected error for malicious ColumnMap %+v", c.cm)
			}
		})
	}
}

// =============================================================================
// Read 路径
// =============================================================================

func TestStore_CreateAndGetByNo(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	spec := orderflow.OrderSpec{
		OrderNo:       "ORD-1",
		OrderToken:    "TOK-1",
		UserID:        1001,
		Status:        orderflow.StatusPending,
		ProductID:     2001,
		ProductTitle:  "VIP",
		PayAmount:     9900,
		OriginalPrice: 9900,
		PayMethod:     "wechat",
		ExpireAt:      time.Now().Add(30 * time.Minute),
	}
	created, err := s.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID == 0 {
		t.Error("expected auto-incremented ID")
	}

	got, hit, err := s.GetByNo(ctx, "ORD-1")
	if err != nil {
		t.Fatalf("GetByNo: %v", err)
	}
	if !hit {
		t.Fatal("expected hit")
	}
	if got.OrderNoCol != spec.OrderNo || got.UserIDCol != spec.UserID {
		t.Errorf("got %+v, want matching %+v", got, spec)
	}
}

func TestStore_GetByNo_NotFound(t *testing.T) {
	s, _ := newTestStore(t)
	_, hit, err := s.GetByNo(context.Background(), "UNKNOWN")
	if err != nil {
		t.Fatalf("GetByNo: %v", err)
	}
	if hit {
		t.Fatal("expected miss for unknown order")
	}
}

func TestStore_GetByToken(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	seedOrder(t, s, &orderRow{
		OrderNoCol: "N", OrderTokenCol: "T", UserIDCol: 1,
		StatusCol: orderflow.StatusPending, ProductIDCol: 1,
		ExpireAtCol: time.Now().Add(time.Hour), PayAmountCol: 1, OriginalPriceCol: 1,
	})

	got, hit, err := s.GetByToken(ctx, "T")
	if err != nil {
		t.Fatalf("GetByToken: %v", err)
	}
	if !hit || got.OrderNoCol != "N" {
		t.Errorf("got %+v hit=%v", got, hit)
	}
}

func TestStore_ListByUser(t *testing.T) {
	s, _ := newTestStore(t)
	base := time.Now().Add(time.Hour)
	for i, uid := range []int64{1001, 1001, 1002, 1001} {
		seedOrder(t, s, &orderRow{
			OrderNoCol: fmt.Sprintf("N-%d", i), OrderTokenCol: fmt.Sprintf("T-%d", i),
			UserIDCol: uid, StatusCol: orderflow.StatusPending, ProductIDCol: 1,
			ExpireAtCol: base, PayAmountCol: 1, OriginalPriceCol: 1,
		})
	}

	got, err := s.ListByUser(context.Background(), 1001)
	if err != nil {
		t.Fatalf("ListByUser: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("ListByUser len = %d, want 3", len(got))
	}

	empty, err := s.ListByUser(context.Background(), 9999)
	if err != nil {
		t.Fatalf("ListByUser empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("empty = %+v, want []", empty)
	}
}

func TestStore_FindPendingByUserAndProduct(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	seedOrder(t, s, &orderRow{
		OrderNoCol: "P1", OrderTokenCol: "TP1", UserIDCol: 1, ProductIDCol: 2,
		StatusCol: orderflow.StatusPending, ExpireAtCol: time.Now().Add(time.Hour),
		PayAmountCol: 1, OriginalPriceCol: 1,
	})

	got, hit, err := s.FindPendingByUserAndProduct(ctx, 1, 2)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if !hit || got.OrderNoCol != "P1" {
		t.Errorf("got %+v hit=%v", got, hit)
	}

	// 同用户不同商品 → miss
	_, hit, _ = s.FindPendingByUserAndProduct(ctx, 1, 99)
	if hit {
		t.Error("expected miss for different product")
	}

	// 把订单推进到 Paid，再查应该 miss（仅返回 Pending）
	_, err = s.CASConfirmPaid(ctx, "P1", "TXN", time.Now())
	if err != nil {
		t.Fatalf("CASConfirmPaid: %v", err)
	}
	_, hit, _ = s.FindPendingByUserAndProduct(ctx, 1, 2)
	if hit {
		t.Error("expected miss after confirm paid")
	}
}

func TestStore_FindExpiredPending(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	seedOrder(t, s, &orderRow{
		OrderNoCol: "EXPIRED-1", OrderTokenCol: "TE1", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPending, ExpireAtCol: now.Add(-time.Hour),
		PayAmountCol: 1, OriginalPriceCol: 1,
	})
	seedOrder(t, s, &orderRow{
		OrderNoCol: "EXPIRED-2", OrderTokenCol: "TE2", UserIDCol: 1, ProductIDCol: 2,
		StatusCol: orderflow.StatusPending, ExpireAtCol: now.Add(-time.Minute),
		PayAmountCol: 1, OriginalPriceCol: 1,
	})
	seedOrder(t, s, &orderRow{
		OrderNoCol: "FUTURE", OrderTokenCol: "TF", UserIDCol: 1, ProductIDCol: 3,
		StatusCol: orderflow.StatusPending, ExpireAtCol: now.Add(time.Hour),
		PayAmountCol: 1, OriginalPriceCol: 1,
	})
	// 已支付不算
	seedOrder(t, s, &orderRow{
		OrderNoCol: "PAID", OrderTokenCol: "TPAID", UserIDCol: 1, ProductIDCol: 4,
		StatusCol: orderflow.StatusPaid, ExpireAtCol: now.Add(-time.Hour),
		PayAmountCol: 1, OriginalPriceCol: 1,
	})

	got, err := s.FindExpiredPending(ctx, 10)
	if err != nil {
		t.Fatalf("FindExpiredPending: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %v, want 2 (EXPIRED-1, EXPIRED-2)", got)
	}

	// Limit 应该生效
	limited, err := s.FindExpiredPending(ctx, 1)
	if err != nil {
		t.Fatalf("FindExpiredPending: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("limited = %v, want 1", limited)
	}
}

func TestStore_FindPaidUndelivered(t *testing.T) {
	s, _ := newTestStore(t)
	now := time.Now().Add(time.Hour)
	seedOrder(t, s, &orderRow{
		OrderNoCol: "P1", OrderTokenCol: "T1", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPaid, ExpireAtCol: now,
		PayAmountCol: 1, OriginalPriceCol: 1,
	})
	seedOrder(t, s, &orderRow{
		OrderNoCol: "D1", OrderTokenCol: "T2", UserIDCol: 1, ProductIDCol: 2,
		StatusCol: orderflow.StatusDelivered, ExpireAtCol: now,
		PayAmountCol: 1, OriginalPriceCol: 1,
	})

	got, err := s.FindPaidUndelivered(context.Background(), 10)
	if err != nil {
		t.Fatalf("FindPaidUndelivered: %v", err)
	}
	if len(got) != 1 || got[0] != "P1" {
		t.Errorf("got %v, want [P1]", got)
	}
}

// =============================================================================
// CAS 路径（核心并发保障）
// =============================================================================

func TestStore_CASClose_PendingToClosed(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	seedOrder(t, s, &orderRow{
		OrderNoCol: "N", OrderTokenCol: "T", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPending, ExpireAtCol: time.Now().Add(time.Hour),
		PayAmountCol: 1, OriginalPriceCol: 1,
	})

	affected, err := s.CASClose(ctx, "N")
	if err != nil {
		t.Fatalf("CASClose: %v", err)
	}
	if affected != 1 {
		t.Fatalf("affected = %d, want 1", affected)
	}

	got, _, _ := s.GetByNo(ctx, "N")
	if got.StatusCol != orderflow.StatusClosed {
		t.Errorf("status = %v, want Closed", got.StatusCol)
	}

	// 再次 CAS 应返回 0（已经不是 Pending）
	affected, err = s.CASClose(ctx, "N")
	if err != nil {
		t.Fatalf("CASClose second: %v", err)
	}
	if affected != 0 {
		t.Errorf("second CAS affected = %d, want 0", affected)
	}
}

func TestStore_CASConfirmPaid(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	seedOrder(t, s, &orderRow{
		OrderNoCol: "N", OrderTokenCol: "T", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPending, ExpireAtCol: time.Now().Add(time.Hour),
		PayAmountCol: 1, OriginalPriceCol: 1,
	})

	paidAt := time.Now().Truncate(time.Second)
	affected, err := s.CASConfirmPaid(ctx, "N", "TXN-123", paidAt)
	if err != nil {
		t.Fatalf("CASConfirmPaid: %v", err)
	}
	if affected != 1 {
		t.Fatalf("affected = %d, want 1", affected)
	}

	got, _, _ := s.GetByNo(ctx, "N")
	if got.StatusCol != orderflow.StatusPaid {
		t.Errorf("status = %v, want Paid", got.StatusCol)
	}
	if got.TradeNoCol != "TXN-123" {
		t.Errorf("trade_no = %q, want TXN-123", got.TradeNoCol)
	}
	if got.PaidAtCol == nil {
		t.Fatal("paid_at not set")
	}

	// 不是 Pending 时 CAS 返回 0
	affected, _ = s.CASConfirmPaid(ctx, "N", "TXN-456", time.Now())
	if affected != 0 {
		t.Errorf("second CAS affected = %d, want 0", affected)
	}
}

func TestStore_CASReopenPaid(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	seedOrder(t, s, &orderRow{
		OrderNoCol: "N", OrderTokenCol: "T", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPending, ExpireAtCol: time.Now().Add(time.Hour),
		PayAmountCol: 1, OriginalPriceCol: 1,
	})

	// Pending 状态下 Reopen 应该失败（0 行）
	affected, _ := s.CASReopenPaid(ctx, "N", "TXN", time.Now())
	if affected != 0 {
		t.Errorf("reopen from Pending: affected = %d, want 0", affected)
	}

	// 先关闭
	_, _ = s.CASClose(ctx, "N")
	// 现在 Closed → Reopen 到 Paid
	affected, err := s.CASReopenPaid(ctx, "N", "TXN", time.Now())
	if err != nil {
		t.Fatalf("CASReopenPaid: %v", err)
	}
	if affected != 1 {
		t.Fatalf("affected = %d, want 1", affected)
	}
	got, _, _ := s.GetByNo(ctx, "N")
	if got.StatusCol != orderflow.StatusPaid {
		t.Errorf("status = %v, want Paid", got.StatusCol)
	}
}

// =============================================================================
// FinalizePaidOrder 事务语义
// =============================================================================

func TestStore_FinalizePaidOrder(t *testing.T) {
	s, db := newTestStore(t)
	ctx := context.Background()
	seedOrder(t, s, &orderRow{
		OrderNoCol: "N", OrderTokenCol: "T", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPending, ExpireAtCol: time.Now().Add(time.Hour),
		PayAmountCol: 9900, OriginalPriceCol: 9900,
	})
	_, _ = s.CASConfirmPaid(ctx, "N", "TXN", time.Now())
	order, _, _ := s.GetByNo(ctx, "N")

	bill := orderflow.BillSpec{
		UserID: 1, OrderNo: "N", TradeNo: "TXN", ProductID: 1,
		PayAmount: 9900, OriginalPrice: 9900, PaidAt: time.Now(),
	}
	if err := s.FinalizePaidOrder(ctx, order, bill); err != nil {
		t.Fatalf("FinalizePaidOrder: %v", err)
	}

	// 交叉验证 1：订单推进到 Delivered，delivered_at 写入
	got, _, _ := s.GetByNo(ctx, "N")
	if got.StatusCol != orderflow.StatusDelivered {
		t.Errorf("status = %v, want Delivered", got.StatusCol)
	}
	if got.DeliveredAtCol == nil {
		t.Error("delivered_at not set")
	}

	// 交叉验证 2：账单表有 1 行对应订单号
	var billCount int64
	if err := db.Table("order_bills").Where("order_no = ?", "N").Count(&billCount).Error; err != nil {
		t.Fatalf("count bills: %v", err)
	}
	if billCount != 1 {
		t.Errorf("bill count = %d, want 1", billCount)
	}
}

// Finalize 要求订单处于 Paid 状态，非 Paid 应报错
func TestStore_FinalizePaidOrder_RequiresPaid(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	order := seedOrder(t, s, &orderRow{
		OrderNoCol: "N", OrderTokenCol: "T", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPending, ExpireAtCol: time.Now().Add(time.Hour),
		PayAmountCol: 9900, OriginalPriceCol: 9900,
	})

	err := s.FinalizePaidOrder(ctx, order, orderflow.BillSpec{OrderNo: "N", PaidAt: time.Now()})
	if err == nil {
		t.Fatal("expected error for non-Paid finalize")
	}

	// 订单状态应保持 Pending（事务回滚）
	got, _, _ := s.GetByNo(ctx, "N")
	if got.StatusCol != orderflow.StatusPending {
		t.Errorf("status = %v, want Pending (finalize should have rolled back)", got.StatusCol)
	}
}

// FinalizeExtra 钩子报错应回滚整个事务（订单状态不变 + 账单不写入）
func TestStore_FinalizePaidOrder_ExtraHookErrorRollsBack(t *testing.T) {
	extraErr := errors.New("extra failed: vip service down")
	s, _ := newTestStore(t, func(c *gormstore.Config[*orderRow, orderRow]) {
		c.FinalizeExtra = func(tx *gorm.DB, _ *orderRow, _ *gormstore.OrderBill) error {
			return extraErr
		}
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
	if err == nil {
		t.Fatal("expected error from FinalizeExtra")
	}
	if !errors.Is(err, extraErr) {
		t.Errorf("err = %v, want wraps %v", err, extraErr)
	}

	// 订单应该回到 Paid（未被推进到 Delivered）
	got, _, _ := s.GetByNo(ctx, "N")
	if got.StatusCol != orderflow.StatusPaid {
		t.Errorf("status = %v, want Paid (rolled back)", got.StatusCol)
	}
}

// FinalizeExtra 钩子成功时，能读到事务内刚写入的 bill（证明同 tx）
func TestStore_FinalizePaidOrder_ExtraHookSeesBillInTx(t *testing.T) {
	var seen bool
	s, _ := newTestStore(t, func(c *gormstore.Config[*orderRow, orderRow]) {
		c.FinalizeExtra = func(tx *gorm.DB, _ *orderRow, bill *gormstore.OrderBill) error {
			// 钩子应该能在事务内看到刚插入的账单（按 ID 查）
			if bill.ID == 0 {
				return errors.New("bill.ID not populated by Create")
			}
			var count int64
			if err := tx.Table("order_bills").Where("id = ?", bill.ID).Count(&count).Error; err != nil {
				return err
			}
			if count != 1 {
				return fmt.Errorf("expected 1 bill in tx, got %d", count)
			}
			seen = true
			return nil
		}
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
		UserID: 1, OrderNo: "N", PaidAt: time.Now(),
	}); err != nil {
		t.Fatalf("FinalizePaidOrder: %v", err)
	}
	if !seen {
		t.Error("FinalizeExtra hook not invoked")
	}
}

// =============================================================================
// UpdateByOrderNo & Logs
// =============================================================================

func TestStore_UpdateByOrderNo(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	seedOrder(t, s, &orderRow{
		OrderNoCol: "N", OrderTokenCol: "T", UserIDCol: 1, ProductIDCol: 1,
		StatusCol: orderflow.StatusPending, ExpireAtCol: time.Now().Add(time.Hour),
		PayAmountCol: 1, OriginalPriceCol: 1,
	})

	err := s.UpdateByOrderNo(ctx, "N", map[string]any{
		"product_title": "Updated Title",
	})
	if err != nil {
		t.Fatalf("UpdateByOrderNo: %v", err)
	}

	got, _, _ := s.GetByNo(ctx, "N")
	if got.ProductTitleCol != "Updated Title" {
		t.Errorf("title = %q, want Updated Title", got.ProductTitleCol)
	}

	// 空 updates 直接返回 nil（不跑 SQL）
	if err := s.UpdateByOrderNo(ctx, "N", nil); err != nil {
		t.Errorf("empty updates: %v", err)
	}
}

func TestStore_AppendLogAndList(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	base := time.Now().Truncate(time.Second)
	entries := []orderflow.LogEntry{
		{OrderNo: "N", UserID: 1, FromStatus: orderflow.StatusPending, ToStatus: orderflow.StatusPending, Actor: "system", Remark: "created", CreatedAt: base},
		{OrderNo: "N", UserID: 1, FromStatus: orderflow.StatusPending, ToStatus: orderflow.StatusPaid, Actor: "system", Remark: "paid", CreatedAt: base.Add(time.Second)},
		{OrderNo: "OTHER", UserID: 1, FromStatus: orderflow.StatusPending, ToStatus: orderflow.StatusClosed, Actor: "system", Remark: "closed", CreatedAt: base},
	}
	for _, e := range entries {
		if err := s.AppendLog(ctx, e); err != nil {
			t.Fatalf("AppendLog: %v", err)
		}
	}

	logs, err := s.ListLogsByOrderNo(ctx, "N")
	if err != nil {
		t.Fatalf("ListLogsByOrderNo: %v", err)
	}
	if len(logs) != 2 {
		t.Fatalf("logs = %d, want 2", len(logs))
	}
	// 按 created_at 升序
	if logs[0].Remark != "created" || logs[1].Remark != "paid" {
		t.Errorf("order: [%q, %q], want [created, paid]", logs[0].Remark, logs[1].Remark)
	}
}

func TestStore_AppendLog_DefaultsCreatedAt(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	// CreatedAt 零值时 driver 应自动填当前时间
	before := time.Now()
	if err := s.AppendLog(ctx, orderflow.LogEntry{
		OrderNo: "N", UserID: 1, FromStatus: orderflow.StatusPending, ToStatus: orderflow.StatusClosed,
	}); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}
	after := time.Now()

	logs, _ := s.ListLogsByOrderNo(ctx, "N")
	if len(logs) != 1 {
		t.Fatalf("logs = %d", len(logs))
	}
	if logs[0].CreatedAt.Before(before.Add(-time.Second)) || logs[0].CreatedAt.After(after.Add(time.Second)) {
		t.Errorf("CreatedAt = %v, not in [%v, %v]", logs[0].CreatedAt, before, after)
	}
}

// =============================================================================
// ColumnMap 自定义列名
// =============================================================================

type altOrderRow struct {
	ID          uint64                `gorm:"primaryKey;autoIncrement"`
	OrderCode   string                `gorm:"column:order_code;uniqueIndex;size:64;not null"`
	UserRef     int64                 `gorm:"column:user_ref;not null"`
	StateCode   orderflow.OrderStatus `gorm:"column:state_code;not null"`
	Tok         string                `gorm:"column:tok;uniqueIndex;size:64;not null"`
	Prod        uint64                `gorm:"column:prod;not null"`
	Amt         int64                 `gorm:"column:amt;not null"`
	OriAmt      int64                 `gorm:"column:ori_amt;not null"`
	Exp         time.Time             `gorm:"column:exp;not null"`
	Txn         string                `gorm:"column:txn;size:128;not null;default:''"`
	PaidTs      *time.Time            `gorm:"column:paid_ts"`
	DeliveredTs *time.Time            `gorm:"column:delivered_ts"`
	UpdatedTs   time.Time             `gorm:"column:updated_ts"`
}

func (altOrderRow) TableName() string { return "alt_orders" }

func (o *altOrderRow) OrderNo() string               { return o.OrderCode }
func (o *altOrderRow) OrderToken() string            { return o.Tok }
func (o *altOrderRow) UserID() int64                 { return o.UserRef }
func (o *altOrderRow) Status() orderflow.OrderStatus { return o.StateCode }
func (o *altOrderRow) ProductID() uint64             { return o.Prod }
func (o *altOrderRow) ProductType() string           { return "" }
func (o *altOrderRow) ProductTitle() string          { return "" }
func (o *altOrderRow) PayMethod() string             { return "" }
func (o *altOrderRow) PayAmount() int64              { return o.Amt }
func (o *altOrderRow) OriginalPrice() int64          { return o.OriAmt }
func (o *altOrderRow) TradeNo() string               { return o.Txn }
func (o *altOrderRow) ExpireAt() time.Time           { return o.Exp }
func (o *altOrderRow) PaidAt() (time.Time, bool) {
	if o.PaidTs == nil {
		return time.Time{}, false
	}
	return *o.PaidTs, true
}
func (o *altOrderRow) Extra() map[string]any { return nil }

func TestStore_ColumnMap_CustomNames(t *testing.T) {
	dsn := "file:gormstore_colmap?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.AutoMigrate(&altOrderRow{}, &gormstore.OrderBill{}, &gormstore.OrderLog{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
	})

	s, err := gormstore.New[*altOrderRow, altOrderRow](gormstore.Config[*altOrderRow, altOrderRow]{
		DB:         db,
		OrderTable: "alt_orders",
		BillTable:  "order_bills",
		LogTable:   "order_logs",
		ColumnMap: gormstore.ColumnMap{
			OrderNo:     "order_code",
			OrderToken:  "tok",
			UserID:      "user_ref",
			ProductID:   "prod",
			Status:      "state_code",
			TradeNo:     "txn",
			PaidAt:      "paid_ts",
			DeliveredAt: "delivered_ts",
			ExpireAt:    "exp",
			UpdatedAt:   "updated_ts",
		},
		Wrap: func(m *altOrderRow) *altOrderRow { return m },
		BuildModel: func(spec orderflow.OrderSpec) *altOrderRow {
			return &altOrderRow{
				OrderCode: spec.OrderNo,
				Tok:       spec.OrderToken,
				UserRef:   spec.UserID,
				StateCode: spec.Status,
				Prod:      spec.ProductID,
				Amt:       spec.PayAmount,
				OriAmt:    spec.OriginalPrice,
				Exp:       spec.ExpireAt,
				UpdatedTs: time.Now(),
			}
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	_, err = s.Create(ctx, orderflow.OrderSpec{
		OrderNo: "ORD", OrderToken: "TOK", UserID: 1, ProductID: 2,
		Status: orderflow.StatusPending, PayAmount: 100, OriginalPrice: 100,
		ExpireAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// 所有主要路径都用自定义列名
	got, hit, err := s.GetByNo(ctx, "ORD")
	if err != nil || !hit {
		t.Fatalf("GetByNo: err=%v hit=%v", err, hit)
	}
	if got.OrderCode != "ORD" || got.UserRef != 1 {
		t.Errorf("got %+v", got)
	}

	// CAS 路径也应用 custom 列名
	affected, err := s.CASConfirmPaid(ctx, "ORD", "TXN", time.Now())
	if err != nil {
		t.Fatalf("CASConfirmPaid: %v", err)
	}
	if affected != 1 {
		t.Errorf("affected = %d, want 1", affected)
	}

	got, _, _ = s.GetByNo(ctx, "ORD")
	if got.StateCode != orderflow.StatusPaid || got.Txn != "TXN" {
		t.Errorf("after CAS: %+v", got)
	}
}
