package orderflow

import "testing"

func TestRefundTradeStatus_String(t *testing.T) {
	cases := []struct {
		status RefundTradeStatus
		want   string
	}{
		{RefundTradeStatusPending, "pending"},
		{RefundTradeStatusProcessing, "processing"},
		{RefundTradeStatusSucceeded, "succeeded"},
		{RefundTradeStatusFailed, "failed"},
		{RefundTradeStatus(""), "unknown"},
		{RefundTradeStatus("garbage"), "unknown"},
	}
	for _, c := range cases {
		if got := c.status.String(); got != c.want {
			t.Errorf("RefundTradeStatus(%q).String() = %q, want %q", string(c.status), got, c.want)
		}
	}
}

func TestRefundTradeStatus_IsTerminal(t *testing.T) {
	terminal := []RefundTradeStatus{
		RefundTradeStatusSucceeded,
		RefundTradeStatusFailed,
	}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("expected %q to be terminal", s)
		}
	}

	nonTerminal := []RefundTradeStatus{
		RefundTradeStatusPending,
		RefundTradeStatusProcessing,
		RefundTradeStatus(""),
		RefundTradeStatus("garbage"),
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("expected %q to be non-terminal", s)
		}
	}
}

func TestRefundRequest_ZeroValueIsSafe(t *testing.T) {
	// 零值结构体不应导致下游使用方 panic（例如序列化、日志打印）。
	var req RefundRequest
	if req.OutTradeNo != "" || req.OutRefundNo != "" || req.RefundAmount != 0 ||
		req.TotalAmount != 0 || req.Reason != "" || req.NotifyURL != "" || req.Metadata != nil {
		t.Errorf("RefundRequest zero value not pristine: %+v", req)
	}
}

func TestRefundResponse_ZeroValueIsSafe(t *testing.T) {
	var resp RefundResponse
	if resp.OutRefundNo != "" || resp.GatewayRefundID != "" || resp.Raw != nil {
		t.Errorf("RefundResponse zero value not pristine: %+v", resp)
	}
}

func TestRefundQueryResult_ZeroValueIsSafe(t *testing.T) {
	var r RefundQueryResult
	if r.OutRefundNo != "" || r.GatewayRefundID != "" || r.Status != "" ||
		r.RefundAmount != 0 || !r.SucceededAt.IsZero() || r.Channel != "" || r.Raw != nil {
		t.Errorf("RefundQueryResult zero value not pristine: %+v", r)
	}
}

func TestRefundNotifyResult_ZeroValueIsSafe(t *testing.T) {
	var r RefundNotifyResult
	if r.OutRefundNo != "" || r.GatewayRefundID != "" || r.Status != "" ||
		r.RefundAmount != 0 || !r.SucceededAt.IsZero() || r.Channel != "" || r.Raw != nil {
		t.Errorf("RefundNotifyResult zero value not pristine: %+v", r)
	}
}

// TestRefundQueryAndNotify_FieldParity 锁住 RefundQueryResult / RefundNotifyResult
// 字段对齐契约——两个 struct 必须能用相同的代码路径处理。
//
// 使用 Go 的类型转换语法 `RefundNotifyResult(q)`：当两个 struct 的字段名 / 类型 / 顺序
// 完全一致时编译通过；任一字段漂移则编译失败——比运行时断言更强的约束。
func TestRefundQueryAndNotify_FieldParity(t *testing.T) {
	q := RefundQueryResult{
		OutRefundNo:     "OR-1",
		GatewayRefundID: "GW-1",
		Status:          RefundTradeStatusSucceeded,
		RefundAmount:    100,
		Channel:         Channel("wxpay"),
	}
	n := RefundNotifyResult(q)
	if n.OutRefundNo != q.OutRefundNo || n.GatewayRefundID != q.GatewayRefundID ||
		n.Status != q.Status || n.RefundAmount != q.RefundAmount ||
		n.Channel != q.Channel {
		t.Errorf("Notify and Query field parity broken: notify=%+v query=%+v", n, q)
	}
}
