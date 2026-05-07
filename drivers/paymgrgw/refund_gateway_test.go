package paymgrgw

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gtkit/go-pay/paymgr"
	"github.com/gtkit/orderflow"
)

// 这一组测试覆盖 Refund / QueryRefund / ParseRefundNotify / AckRefundNotify /
// IsIgnorableRefundError 五个方法。fakeProvider 在 gateway_transparent_test.go 中已可编程。

// ----- Refund -----

func TestRefund_SuccessMapsFields(t *testing.T) {
	fp := &fakeProvider{
		channel: fakeChannel,
		refundResp: &paymgr.RefundResponse{
			Channel:      fakeChannel,
			OutRefundNo:  "OR-1",
			RefundID:     "GW-REFUND-1",
			RefundAmount: 100,
		},
	}
	g := newTestGateway(t, fp)

	resp, err := g.Refund(context.Background(), orderflow.Channel(fakeChannel), orderflow.RefundRequest{
		OutTradeNo:    "OUT-1",
		TransactionID: "TXN-1",
		OutRefundNo:   "OR-1",
		RefundAmount:  100,
		TotalAmount:   100,
		Reason:        "客户申请",
		NotifyURL:     "https://example.com/refund-notify",
		Metadata:      map[string]string{"ticket": "T-1"},
	})
	if err != nil {
		t.Fatalf("Refund: %v", err)
	}
	if resp.OutRefundNo != "OR-1" {
		t.Errorf("OutRefundNo = %q", resp.OutRefundNo)
	}
	if resp.GatewayRefundID != "GW-REFUND-1" {
		t.Errorf("GatewayRefundID = %q", resp.GatewayRefundID)
	}
	if resp.RefundAmount != 100 {
		t.Errorf("RefundAmount = %d, want 100", resp.RefundAmount)
	}
	if resp.Channel != orderflow.Channel(fakeChannel) {
		t.Errorf("Channel = %q", resp.Channel)
	}
	// fakeChannel 不是 alipay/wechat，syncRefundStatus 走 default 分支 → Processing
	if resp.Status != orderflow.RefundTradeStatusProcessing {
		t.Errorf("Status = %q, want processing (default for unknown channel)", resp.Status)
	}
	if resp.Raw != fp.refundResp {
		t.Errorf("Raw should preserve original *paymgr.RefundResponse pointer")
	}

	// 入参映射校验
	if fp.refundGot == nil {
		t.Fatal("fakeProvider.Refund was not invoked")
	}
	if fp.refundGot.OutTradeNo != "OUT-1" || fp.refundGot.TransactionID != "TXN-1" ||
		fp.refundGot.OutRefundNo != "OR-1" ||
		fp.refundGot.RefundAmount != 100 || fp.refundGot.TotalAmount != 100 ||
		fp.refundGot.Reason != "客户申请" || fp.refundGot.NotifyURL != "https://example.com/refund-notify" {
		t.Errorf("Refund req not propagated: %+v", fp.refundGot)
	}
}

// TestRefund_SyncStatusByChannel 验证 syncRefundStatus 的渠道行为契约：
// alipay 同步成功 = succeeded（终态）、wechat = processing（等异步）。
// 这是业务方编排的关键：业务方据此决定是否在 Refund 返回后立即触发反向核销。
func TestRefund_SyncStatusByChannel(t *testing.T) {
	cases := []struct {
		ch         paymgr.Channel
		wantStatus orderflow.RefundTradeStatus
	}{
		{paymgr.ChannelAlipay, orderflow.RefundTradeStatusSucceeded},
		{paymgr.ChannelWechat, orderflow.RefundTradeStatusProcessing},
		{paymgr.Channel("custom-rail"), orderflow.RefundTradeStatusProcessing}, // default 保守处理
	}
	for _, c := range cases {
		fp := &fakeProvider{
			channel: c.ch,
			refundResp: &paymgr.RefundResponse{
				Channel: c.ch, OutRefundNo: "OR-X", RefundID: "GW-X", RefundAmount: 1,
			},
		}
		g := newTestGateway(t, fp)
		resp, err := g.Refund(context.Background(), orderflow.Channel(c.ch),
			orderflow.RefundRequest{OutTradeNo: "OUT-X", OutRefundNo: "OR-X", RefundAmount: 1, TotalAmount: 1})
		if err != nil {
			t.Fatalf("channel %q: %v", c.ch, err)
		}
		if resp.Status != c.wantStatus {
			t.Errorf("channel %q: Status = %q, want %q", c.ch, resp.Status, c.wantStatus)
		}
	}
}

func TestRefund_ErrorIsTransparent(t *testing.T) {
	sentinel := errors.New("network down")
	fp := &fakeProvider{channel: fakeChannel, refundErr: sentinel}
	g := newTestGateway(t, fp)

	resp, err := g.Refund(context.Background(), orderflow.Channel(fakeChannel), orderflow.RefundRequest{
		OutTradeNo: "OUT-1", OutRefundNo: "OR-1", RefundAmount: 1, TotalAmount: 1,
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel, got %v", err)
	}
	if resp.OutRefundNo != "" || resp.GatewayRefundID != "" || resp.Raw != nil {
		t.Errorf("response should be zero on error, got %+v", resp)
	}
}

// ----- QueryRefund -----

func TestQueryRefund_SuccessMapsFields(t *testing.T) {
	refundedAt := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	fp := &fakeProvider{
		channel: fakeChannel,
		queryRefundResp: &paymgr.QueryRefundResponse{
			Channel:       fakeChannel,
			OutTradeNo:    "OUT-1",
			OutRefundNo:   "OR-1",
			RefundID:      "GW-REFUND-1",
			RefundStatus:  paymgr.RefundStatusSuccess,
			RefundAmount:  100,
			TotalAmount:   100,
			RefundedAt:    refundedAt,
			TransactionID: "TXN-1",
		},
	}
	g := newTestGateway(t, fp)

	res, err := g.QueryRefund(context.Background(), orderflow.Channel(fakeChannel), "OR-1")
	if err != nil {
		t.Fatalf("QueryRefund: %v", err)
	}
	if res.OutRefundNo != "OR-1" || res.GatewayRefundID != "GW-REFUND-1" {
		t.Errorf("ID fields not propagated: %+v", res)
	}
	if res.OutTradeNo != "OUT-1" {
		t.Errorf("OutTradeNo = %q, want OUT-1", res.OutTradeNo)
	}
	if res.TransactionID != "TXN-1" {
		t.Errorf("TransactionID = %q, want TXN-1", res.TransactionID)
	}
	if res.Status != orderflow.RefundTradeStatusSucceeded {
		t.Errorf("Status = %q, want succeeded", res.Status)
	}
	if res.RefundAmount != 100 {
		t.Errorf("RefundAmount = %d", res.RefundAmount)
	}
	if res.TotalAmount != 100 {
		t.Errorf("TotalAmount = %d, want 100", res.TotalAmount)
	}
	if !res.SucceededAt.Equal(refundedAt) {
		t.Errorf("SucceededAt = %v, want %v", res.SucceededAt, refundedAt)
	}
	if res.Channel != orderflow.Channel(fakeChannel) {
		t.Errorf("Channel = %q", res.Channel)
	}
	if res.Raw != fp.queryRefundResp {
		t.Error("Raw should preserve original *paymgr.QueryRefundResponse pointer")
	}
	if fp.queryRefundGot == nil || fp.queryRefundGot.OutRefundNo != "OR-1" {
		t.Errorf("QueryRefund req not propagated: %+v", fp.queryRefundGot)
	}
}

func TestQueryRefund_NotFoundMapsToErrRefundNotFound(t *testing.T) {
	fp := &fakeProvider{channel: fakeChannel, queryRefundErr: paymgr.ErrOrderNotFound}
	g := newTestGateway(t, fp)

	res, err := g.QueryRefund(context.Background(), orderflow.Channel(fakeChannel), "OR-X")
	if !errors.Is(err, orderflow.ErrRefundNotFound) {
		t.Errorf("expected ErrRefundNotFound, got %v", err)
	}
	if res.OutRefundNo != "" || res.Status != "" {
		t.Errorf("result should be zero on not-found, got %+v", res)
	}
}

func TestQueryRefund_WechatChannelErrorNotFound(t *testing.T) {
	wechatErr := paymgr.NewChannelError(paymgr.ChannelWechat, "RESOURCE_NOT_EXISTS", "退款单不存在", nil)
	fp := &fakeProvider{channel: paymgr.ChannelWechat, queryRefundErr: wechatErr}
	g := newTestGateway(t, fp)

	_, err := g.QueryRefund(context.Background(), orderflow.Channel(paymgr.ChannelWechat), "OR-X")
	if !errors.Is(err, orderflow.ErrRefundNotFound) {
		t.Errorf("expected ErrRefundNotFound for wechat RESOURCE_NOT_EXISTS, got %v", err)
	}
}

func TestQueryRefund_RefundStatusMapping(t *testing.T) {
	cases := []struct {
		paymgrStatus paymgr.RefundStatus
		want         orderflow.RefundTradeStatus
	}{
		{paymgr.RefundStatusProcessing, orderflow.RefundTradeStatusProcessing},
		{paymgr.RefundStatusSuccess, orderflow.RefundTradeStatusSucceeded},
		{paymgr.RefundStatusClosed, orderflow.RefundTradeStatusFailed},                // 渠道关闭 / 终态失败
		{paymgr.RefundStatusError, orderflow.RefundTradeStatusFailed},                 // 未知错误 / 终态失败
		{paymgr.RefundStatusAbnormal, orderflow.RefundTradeStatusUnknown},             // 需人工介入 / 非终态
		{paymgr.RefundStatus("totally-unknown"), orderflow.RefundTradeStatusUnknown},  // driver 未识别 → 业务方告警
	}
	for _, c := range cases {
		fp := &fakeProvider{
			channel: fakeChannel,
			queryRefundResp: &paymgr.QueryRefundResponse{
				Channel:      fakeChannel,
				OutRefundNo:  "OR-X",
				RefundStatus: c.paymgrStatus,
			},
		}
		g := newTestGateway(t, fp)
		res, err := g.QueryRefund(context.Background(), orderflow.Channel(fakeChannel), "OR-X")
		if err != nil {
			t.Fatalf("status %q: QueryRefund err: %v", c.paymgrStatus, err)
		}
		if res.Status != c.want {
			t.Errorf("paymgr %q → orderflow %q, want %q", c.paymgrStatus, res.Status, c.want)
		}
	}
}

// ----- ParseRefundNotify -----

func TestParseRefundNotify_SuccessMapsFields(t *testing.T) {
	refundedAt := time.Date(2026, 5, 7, 13, 0, 0, 0, time.UTC)
	fp := &fakeProvider{
		channel: fakeChannel,
		refundNotifyResp: &paymgr.RefundNotifyResult{
			Channel:             fakeChannel,
			OutTradeNo:          "OUT-N",
			TransactionID:       "TXN-N",
			OutRefundNo:         "OR-N",
			RefundID:            "GW-REFUND-N",
			RefundStatus:        paymgr.RefundStatusSuccess,
			RefundAmount:        200,
			TotalAmount:         200,
			RefundedAt:          refundedAt,
			UserReceivedAccount: "招商银行信用卡0403",
		},
	}
	g := newTestGateway(t, fp)

	req := httptest.NewRequest(http.MethodPost, "/refund-notify", nil)
	res, err := g.ParseRefundNotify(context.Background(), orderflow.Channel(fakeChannel), req)
	if err != nil {
		t.Fatalf("ParseRefundNotify: %v", err)
	}
	if res.OutRefundNo != "OR-N" || res.GatewayRefundID != "GW-REFUND-N" {
		t.Errorf("ID fields not propagated: %+v", res)
	}
	if res.OutTradeNo != "OUT-N" {
		t.Errorf("OutTradeNo = %q, want OUT-N", res.OutTradeNo)
	}
	if res.TransactionID != "TXN-N" {
		t.Errorf("TransactionID = %q, want TXN-N", res.TransactionID)
	}
	if res.TotalAmount != 200 {
		t.Errorf("TotalAmount = %d, want 200", res.TotalAmount)
	}
	if res.UserReceivedAccount != "招商银行信用卡0403" {
		t.Errorf("UserReceivedAccount = %q, want 招商银行信用卡0403", res.UserReceivedAccount)
	}
	if res.Status != orderflow.RefundTradeStatusSucceeded {
		t.Errorf("Status = %q", res.Status)
	}
	if res.RefundAmount != 200 {
		t.Errorf("RefundAmount = %d", res.RefundAmount)
	}
	if !res.SucceededAt.Equal(refundedAt) {
		t.Errorf("SucceededAt = %v", res.SucceededAt)
	}
	if res.Channel != orderflow.Channel(fakeChannel) {
		t.Errorf("Channel = %q", res.Channel)
	}
	rawNotify, ok := res.Raw.(*paymgr.RefundNotifyResult)
	if !ok {
		t.Fatal("Raw should preserve the original *paymgr.RefundNotifyResult pointer")
	}
	if rawNotify.OutRefundNo != "OR-N" {
		t.Errorf("Raw content lost: %+v", rawNotify)
	}
}

func TestParseRefundNotify_SignatureFailureIsTransparent(t *testing.T) {
	sentinel := errors.New("signature verification failed")
	fp := &fakeProvider{channel: fakeChannel, refundNotifyErr: sentinel}
	g := newTestGateway(t, fp)

	req := httptest.NewRequest(http.MethodPost, "/refund-notify", nil)
	res, err := g.ParseRefundNotify(context.Background(), orderflow.Channel(fakeChannel), req)
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
	// 验签失败时必须返回零值——RefundGateway 的安全契约依赖这一点
	if res.OutRefundNo != "" || res.Status != "" || res.Raw != nil {
		t.Errorf("result must be zero on signature failure, got %+v", res)
	}
}

// ----- AckRefundNotify -----

func TestAckRefundNotify_Success(t *testing.T) {
	fp := &fakeProvider{channel: fakeChannel}
	g := newTestGateway(t, fp)

	rec := httptest.NewRecorder()
	if err := g.AckRefundNotify(orderflow.Channel(fakeChannel), rec); err != nil {
		t.Fatalf("AckRefundNotify: %v", err)
	}
	if !fp.ackCalled {
		t.Error("Provider.ACKNotify was not invoked")
	}
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Errorf("response not propagated: code=%d body=%q", rec.Code, rec.Body.String())
	}
}

// ----- IsIgnorableRefundError -----

func TestIsIgnorableRefundError(t *testing.T) {
	g := New(paymgr.NewManager())

	cases := []struct {
		name string
		ch   orderflow.Channel
		err  error
		want bool
	}{
		{"nil error", "wxpay", nil, false},
		{"non channel error", "wxpay", errors.New("random"), false},
		{
			"wechat duplicate",
			orderflow.Channel(paymgr.ChannelWechat),
			paymgr.NewChannelError(paymgr.ChannelWechat, "DUPLICATE_REQUEST", "重复", nil),
			true,
		},
		{
			"wechat resource exists",
			orderflow.Channel(paymgr.ChannelWechat),
			paymgr.NewChannelError(paymgr.ChannelWechat, "RESOURCE_ALREADY_EXISTS", "退款单已存在", nil),
			true,
		},
		{
			"wechat unknown channel error",
			orderflow.Channel(paymgr.ChannelWechat),
			paymgr.NewChannelError(paymgr.ChannelWechat, "SOMETHING_ELSE", "x", nil),
			false,
		},
		{
			"alipay duplicate",
			orderflow.Channel(paymgr.ChannelAlipay),
			paymgr.NewChannelError(paymgr.ChannelAlipay, "ACQ.DUPLICATE_REFUND_REQUEST", "重复退款", nil),
			true,
		},
		{
			"alipay refund limit",
			orderflow.Channel(paymgr.ChannelAlipay),
			paymgr.NewChannelError(paymgr.ChannelAlipay, "ACQ.TRADE_HAS_REFUND_LIMIT", "金额一致幂等", nil),
			true,
		},
		{
			"alipay unrelated",
			orderflow.Channel(paymgr.ChannelAlipay),
			paymgr.NewChannelError(paymgr.ChannelAlipay, "ACQ.SYSTEM_ERROR", "x", nil),
			false,
		},
		{
			"unknown channel",
			"unknown",
			paymgr.NewChannelError("unknown", "DUPLICATE_REQUEST", "x", nil),
			false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := g.IsIgnorableRefundError(c.ch, c.err); got != c.want {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
	}
}

// ----- 接口断言（编译期保证） -----

func TestGateway_ImplementsRefundGateway(t *testing.T) {
	var _ orderflow.RefundGateway = (*Gateway)(nil)
}
