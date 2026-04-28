package orderflow

import "testing"

func TestOrderStatus_String(t *testing.T) {
	cases := []struct {
		s    OrderStatus
		want string
	}{
		{StatusPending, "pending"},
		{StatusPaid, "paid"},
		{StatusDelivered, "delivered"},
		{StatusCompleted, "completed"},
		{StatusClosed, "closed"},
		{StatusCancelled, "cancelled"},
		{OrderStatus(127), "unknown"},
		{OrderStatus(-1), "unknown"},
	}
	for _, c := range cases {
		if got := c.s.String(); got != c.want {
			t.Errorf("OrderStatus(%d).String() = %q, want %q", c.s, got, c.want)
		}
	}
}

func TestOrderStatus_NumericValues(t *testing.T) {
	want := map[OrderStatus]int8{
		StatusPending:   0,
		StatusPaid:      10,
		StatusDelivered: 20,
		StatusCompleted: 30,
		StatusClosed:    40,
		StatusCancelled: 50,
	}
	for s, n := range want {
		if int8(s) != n {
			t.Errorf("status %s = %d, want %d", s, s, n)
		}
	}
}

func TestOrderStatus_ZeroValueIsPending(t *testing.T) {
	var s OrderStatus
	if s != StatusPending {
		t.Errorf("zero value = %d, want StatusPending (0)", s)
	}
	if s.String() != "pending" {
		t.Errorf("zero value String() = %q, want %q", s.String(), "pending")
	}
}

func TestOrderStatus_IsTerminal(t *testing.T) {
	terminal := []OrderStatus{
		StatusCompleted,
		StatusClosed,
		StatusCancelled,
	}
	nonTerminal := []OrderStatus{
		StatusPending,
		StatusPaid,
		StatusDelivered,
	}

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
	terminals := []OrderStatus{
		StatusCompleted,
		StatusClosed,
		StatusCancelled,
	}
	allTargets := []OrderStatus{
		StatusPending,
		StatusPaid,
		StatusDelivered,
		StatusCompleted,
		StatusClosed,
		StatusCancelled,
	}
	for _, from := range terminals {
		for _, to := range allTargets {
			if from.CanTransitionTo(to) {
				t.Errorf("%s -> %s should be disallowed (terminal)", from, to)
			}
		}
	}

	// 非法：Pending 不可跳 Delivered / Completed
	for _, to := range []OrderStatus{StatusDelivered, StatusCompleted} {
		if StatusPending.CanTransitionTo(to) {
			t.Errorf("Pending -> %s should be disallowed", to)
		}
	}

	// 非法：Paid 不可直跳 Completed / Cancelled
	for _, to := range []OrderStatus{StatusCompleted, StatusCancelled} {
		if StatusPaid.CanTransitionTo(to) {
			t.Errorf("Paid -> %s should be disallowed", to)
		}
	}
}
