package orderflow

import "testing"

func TestOrderStatus_String(t *testing.T) {
	cases := []struct {
		s    OrderStatus
		want string
	}{
		{StatusUnknown, "unknown"},
		{StatusPending, "pending"},
		{StatusPaid, "paid"},
		{StatusDelivered, "delivered"},
		{StatusCompleted, "completed"},
		{StatusClosed, "closed"},
		{StatusCancelled, "cancelled"},
		{OrderStatus(99), "unknown"},
	}
	for _, c := range cases {
		if got := c.s.String(); got != c.want {
			t.Errorf("OrderStatus(%d).String() = %q, want %q", c.s, got, c.want)
		}
	}
}

func TestOrderStatus_IsTerminal(t *testing.T) {
	terminal := []OrderStatus{StatusCompleted, StatusClosed, StatusCancelled}
	nonTerminal := []OrderStatus{StatusUnknown, StatusPending, StatusPaid, StatusDelivered}

	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}

func TestOrderStatus_CanTransitionTo(t *testing.T) {
	// 完整合法跃迁矩阵
	allowed := map[OrderStatus][]OrderStatus{
		StatusPending:   {StatusPaid, StatusClosed, StatusCancelled},
		StatusPaid:      {StatusDelivered, StatusClosed},
		StatusDelivered: {StatusCompleted},
	}
	for from, targets := range allowed {
		for _, to := range targets {
			if !from.CanTransitionTo(to) {
				t.Errorf("%s -> %s should be allowed", from, to)
			}
		}
	}

	// 非法：终态出发禁止任何跃迁
	for _, from := range []OrderStatus{StatusCompleted, StatusClosed, StatusCancelled, StatusUnknown} {
		for _, to := range []OrderStatus{StatusPending, StatusPaid, StatusDelivered, StatusCompleted} {
			if from.CanTransitionTo(to) {
				t.Errorf("%s -> %s should be disallowed", from, to)
			}
		}
	}

	// 非法：Pending 不可跳 Delivered / Completed
	for _, to := range []OrderStatus{StatusDelivered, StatusCompleted} {
		if StatusPending.CanTransitionTo(to) {
			t.Errorf("Pending -> %s should be disallowed", to)
		}
	}

	// 非法：Paid 不可直跳 Completed
	if StatusPaid.CanTransitionTo(StatusCompleted) {
		t.Errorf("Paid -> Completed should be disallowed (must go Delivered first)")
	}
}
