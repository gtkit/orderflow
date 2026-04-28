package worker

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderflow"
)

// DeliveryFallback 周期扫描 Paid 未 Delivered 的订单，调 ReconcilePaid 补偿。

func TestDeliveryFallback_ScanReconcilesPaid(t *testing.T) {
	rig := newTestRig(t)

	paidAt := time.Now().Add(-time.Minute)
	rig.store.seed(&testOrder{
		orderNo:    "P1",
		orderToken: "t-p1",
		status:     orderflow.StatusPaid,
		payMethod:  orderflow.PayMethodWechat,
		payAmount:  100,
		tradeNo:    "TXN-1",
		paidAt:     &paidAt,
	})
	rig.store.seed(&testOrder{
		orderNo:    "P2",
		orderToken: "t-p2",
		status:     orderflow.StatusPaid,
		payMethod:  orderflow.PayMethodWechat,
		payAmount:  200,
		tradeNo:    "TXN-2",
		paidAt:     &paidAt,
	})
	// Delivered 的不扫
	rig.store.seed(&testOrder{
		orderNo: "D1",
		status:  orderflow.StatusDelivered,
	})

	f := NewDeliveryFallback(rig.engine, DeliveryFallbackOptions{BatchSize: 10})
	f.scan(context.Background())

	mustEqual(t, rig.store.byNo["P1"].status, orderflow.StatusDelivered, "P1 finalized")
	mustEqual(t, rig.store.byNo["P2"].status, orderflow.StatusDelivered, "P2 finalized")
	mustEqual(t, rig.store.byNo["D1"].status, orderflow.StatusDelivered, "D1 unchanged")

	// 两单各写了一份 bill
	mustLen(t, rig.store.bills, 2, "bills written")
	mustEqual(t, rig.store.ReconcileCompletes, 2, "Finalize calls")
}

func TestDeliveryFallback_SkipsWhenMetadataMissing(t *testing.T) {
	rig := newTestRig(t)
	// Paid 但没 tradeNo/paidAt → ReconcilePaid 报错，Engine 会 log 但不 panic
	rig.store.seed(&testOrder{
		orderNo: "NO-META",
		status:  orderflow.StatusPaid,
		// tradeNo 空，paidAt nil
	})

	f := NewDeliveryFallback(rig.engine, DeliveryFallbackOptions{BatchSize: 10})
	// scan 不应 panic，即便底层 ReconcilePaid 返回错误
	f.scan(context.Background())

	// 订单状态不变
	mustEqual(t, rig.store.byNo["NO-META"].status, orderflow.StatusPaid, "status unchanged")
	mustEqual(t, rig.store.ReconcileCompletes, 0, "no Finalize")
}

func TestDeliveryFallback_RunStopsOnContextCancel(t *testing.T) {
	rig := newTestRig(t)
	f := NewDeliveryFallback(rig.engine, DeliveryFallbackOptions{Interval: 10 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.Run(ctx)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DeliveryFallback.Run did not stop")
	}
}
