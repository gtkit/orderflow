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
		{RefundTradeStatusUnknown, "unknown"},
		{RefundTradeStatus(""), "invalid"},        // 零值非法
		{RefundTradeStatus("garbage"), "invalid"}, // 未声明值非法
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

	// Unknown 必须是**非终态**——这是关键的语义不变量。
	// 业务方据此区分"明确失败 / 状态待定"，不会对 Unknown 误触发反向核销。
	nonTerminal := []RefundTradeStatus{
		RefundTradeStatusPending,
		RefundTradeStatusProcessing,
		RefundTradeStatusUnknown,
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
	if req.OutTradeNo != "" || req.TransactionID != "" || req.OutRefundNo != "" ||
		req.RefundAmount != 0 || req.TotalAmount != 0 || req.Reason != "" ||
		req.NotifyURL != "" || req.Metadata != nil {
		t.Errorf("RefundRequest zero value not pristine: %+v", req)
	}
}

func TestRefundResponse_ZeroValueIsSafe(t *testing.T) {
	var resp RefundResponse
	if resp.OutRefundNo != "" || resp.GatewayRefundID != "" || resp.Status != "" ||
		resp.RefundAmount != 0 || resp.Channel != "" || resp.Raw != nil {
		t.Errorf("RefundResponse zero value not pristine: %+v", resp)
	}
}

func TestRefundQueryResult_ZeroValueIsSafe(t *testing.T) {
	var r RefundQueryResult
	if r.OutRefundNo != "" || r.OutTradeNo != "" || r.TransactionID != "" ||
		r.GatewayRefundID != "" || r.Status != "" || r.RefundAmount != 0 ||
		r.TotalAmount != 0 || !r.SucceededAt.IsZero() || r.Channel != "" || r.Raw != nil {
		t.Errorf("RefundQueryResult zero value not pristine: %+v", r)
	}
}

func TestRefundNotifyResult_ZeroValueIsSafe(t *testing.T) {
	var r RefundNotifyResult
	if r.OutRefundNo != "" || r.OutTradeNo != "" || r.TransactionID != "" ||
		r.GatewayRefundID != "" || r.Status != "" || r.RefundAmount != 0 ||
		r.TotalAmount != 0 || !r.SucceededAt.IsZero() || r.Channel != "" ||
		r.UserReceivedAccount != "" || r.Raw != nil {
		t.Errorf("RefundNotifyResult zero value not pristine: %+v", r)
	}
}

// TestRefundNotify_HasQueryFields 锁住「RefundNotifyResult 包含 RefundQueryResult
// 的全部公共字段」契约——两条路径（主动 Query / 异步通知）的处理代码可以共享字段访问。
// NotifyResult 额外有 UserReceivedAccount（仅微信返回），是有意识的扩展。
//
// 通过手工字段拷贝实现：任一字段名漂移会让本测试编译失败。
func TestRefundNotify_HasQueryFields(t *testing.T) {
	q := RefundQueryResult{
		OutRefundNo:     "OR-1",
		OutTradeNo:      "OUT-1",
		TransactionID:   "TXN-1",
		GatewayRefundID: "GW-1",
		Status:          RefundTradeStatusSucceeded,
		RefundAmount:    100,
		TotalAmount:     200,
		Channel:         Channel("wxpay"),
	}
	n := RefundNotifyResult{
		OutRefundNo:     q.OutRefundNo,
		OutTradeNo:      q.OutTradeNo,
		TransactionID:   q.TransactionID,
		GatewayRefundID: q.GatewayRefundID,
		Status:          q.Status,
		RefundAmount:    q.RefundAmount,
		TotalAmount:     q.TotalAmount,
		SucceededAt:     q.SucceededAt,
		Channel:         q.Channel,
		Raw:             q.Raw,
	}
	if n.OutRefundNo != q.OutRefundNo ||
		n.OutTradeNo != q.OutTradeNo ||
		n.TransactionID != q.TransactionID ||
		n.GatewayRefundID != q.GatewayRefundID ||
		n.Status != q.Status ||
		n.RefundAmount != q.RefundAmount ||
		n.TotalAmount != q.TotalAmount ||
		n.Channel != q.Channel {
		t.Errorf("Notify and Query common fields drift: notify=%+v query=%+v", n, q)
	}
}
