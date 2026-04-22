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

// 这一组测试覆盖 UnifiedOrder / CloseOrder / QueryOrder / ParseNotify / AckNotify
// 五个薄包装方法。策略是用一个可编程的 fake Provider 注册到 paymgr.Manager，
// 然后通过 Gateway 调用验证：
//   1. 请求字段从 orderflow 结构体正确映射到 paymgr 结构体（含 TradeType option）；
//   2. Provider 返回的成功结果字段映射回 orderflow 结构体；
//   3. Provider 返回的错误原样透传给调用方；
//   4. ParseNotify 成功时 Raw 字段保留指向原始 *paymgr.NotifyResult 的指针，避免调用方重新解析。

const fakeChannel paymgr.Channel = "fakepay"

// fakeProvider 是 paymgr.Provider 的可编程实现。每个方法都可以注入期望的返回值 / 错误。
type fakeProvider struct {
	channel paymgr.Channel

	unifiedResp *paymgr.UnifiedOrderResponse
	unifiedErr  error
	unifiedGot  *paymgr.UnifiedOrderRequest

	queryResp *paymgr.QueryOrderResponse
	queryErr  error
	queryGot  *paymgr.QueryOrderRequest

	closeErr error
	closeGot *paymgr.CloseOrderRequest

	notifyResp *paymgr.NotifyResult
	notifyErr  error

	ackCalled bool
}

func (p *fakeProvider) Channel() paymgr.Channel { return p.channel }

func (p *fakeProvider) UnifiedOrder(_ context.Context, req *paymgr.UnifiedOrderRequest) (*paymgr.UnifiedOrderResponse, error) {
	p.unifiedGot = req
	if p.unifiedErr != nil {
		return nil, p.unifiedErr
	}
	return p.unifiedResp, nil
}

func (p *fakeProvider) QueryOrder(_ context.Context, req *paymgr.QueryOrderRequest) (*paymgr.QueryOrderResponse, error) {
	p.queryGot = req
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	return p.queryResp, nil
}

func (p *fakeProvider) CloseOrder(_ context.Context, req *paymgr.CloseOrderRequest) error {
	p.closeGot = req
	return p.closeErr
}

func (p *fakeProvider) Refund(_ context.Context, _ *paymgr.RefundRequest) (*paymgr.RefundResponse, error) {
	return nil, errors.New("fakeProvider.Refund not used by paymgrgw tests")
}

func (p *fakeProvider) ParseNotify(_ context.Context, _ *http.Request) (*paymgr.NotifyResult, error) {
	if p.notifyErr != nil {
		return nil, p.notifyErr
	}
	return p.notifyResp, nil
}

func (p *fakeProvider) ACKNotify(w http.ResponseWriter) {
	p.ackCalled = true
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func newTestGateway(t *testing.T, fp *fakeProvider, opts ...Option) *Gateway {
	t.Helper()
	mgr := paymgr.NewManager()
	mgr.Register(fp)
	return New(mgr, opts...)
}

// ----- UnifiedOrder -----

func TestUnifiedOrder_SuccessMapsFields(t *testing.T) {
	fp := &fakeProvider{
		channel: fakeChannel,
		unifiedResp: &paymgr.UnifiedOrderResponse{
			AppParams: `{"prepay_id":"wx123"}`,
			PrepayID:  "wx123",
		},
	}
	g := newTestGateway(t, fp, WithTradeType(paymgr.TradeTypeH5))

	expire := time.Now().Add(15 * time.Minute)
	resp, err := g.UnifiedOrder(context.Background(), orderflow.Channel(fakeChannel), orderflow.UnifiedOrderRequest{
		OutTradeNo:  "OUT-1",
		TotalAmount: 9900,
		Subject:     "VIP-1M",
		NotifyURL:   "https://example.com/notify",
		ExpireAt:    expire,
		Metadata:    map[string]string{"order_token": "tok-1"},
	})
	if err != nil {
		t.Fatalf("UnifiedOrder: %v", err)
	}
	if resp.AppParams != `{"prepay_id":"wx123"}` {
		t.Errorf("AppParams = %q", resp.AppParams)
	}
	// Raw 必须是 Provider 返回的原始指针，供上层调试 / 扩展用
	if rawResp, ok := resp.Raw.(*paymgr.UnifiedOrderResponse); !ok || rawResp != fp.unifiedResp {
		t.Errorf("Raw should be the *paymgr.UnifiedOrderResponse returned by Provider")
	}

	// 入参透传校验
	got := fp.unifiedGot
	if got == nil {
		t.Fatal("Provider.UnifiedOrder was not invoked")
	}
	if got.OutTradeNo != "OUT-1" || got.TotalAmount != 9900 || got.Subject != "VIP-1M" {
		t.Errorf("request fields not transparently mapped: %+v", got)
	}
	if got.TradeType != paymgr.TradeTypeH5 {
		t.Errorf("WithTradeType(H5) should override default; got %v", got.TradeType)
	}
	if got.NotifyURL != "https://example.com/notify" {
		t.Errorf("NotifyURL = %q", got.NotifyURL)
	}
	if !got.ExpireAt.Equal(expire) {
		t.Errorf("ExpireAt mismatch: got %v want %v", got.ExpireAt, expire)
	}
	if got.Metadata["order_token"] != "tok-1" {
		t.Errorf("Metadata not propagated: %+v", got.Metadata)
	}
}

func TestUnifiedOrder_DefaultTradeTypeIsApp(t *testing.T) {
	fp := &fakeProvider{
		channel:     fakeChannel,
		unifiedResp: &paymgr.UnifiedOrderResponse{},
	}
	g := newTestGateway(t, fp)

	_, err := g.UnifiedOrder(context.Background(), orderflow.Channel(fakeChannel), orderflow.UnifiedOrderRequest{
		OutTradeNo: "OUT-2", TotalAmount: 1, Subject: "x",
		NotifyURL: "https://example.com/notify",
	})
	if err != nil {
		t.Fatalf("UnifiedOrder: %v", err)
	}
	if fp.unifiedGot == nil || fp.unifiedGot.TradeType != paymgr.TradeTypeApp {
		t.Errorf("default TradeType should be App, got %v", fp.unifiedGot.TradeType)
	}
}

func TestUnifiedOrder_ErrorIsTransparent(t *testing.T) {
	sentinel := errors.New("network down")
	fp := &fakeProvider{channel: fakeChannel, unifiedErr: sentinel}
	g := newTestGateway(t, fp)

	resp, err := g.UnifiedOrder(context.Background(), orderflow.Channel(fakeChannel), orderflow.UnifiedOrderRequest{
		OutTradeNo: "OUT-3", TotalAmount: 1, Subject: "x",
		NotifyURL: "https://example.com/notify",
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
	if resp.AppParams != "" || resp.Raw != nil {
		t.Errorf("response should be zero value on error, got %+v", resp)
	}
}

// ----- CloseOrder -----

func TestCloseOrder_Success(t *testing.T) {
	fp := &fakeProvider{channel: fakeChannel}
	g := newTestGateway(t, fp)

	if err := g.CloseOrder(context.Background(), orderflow.Channel(fakeChannel), "OUT-9"); err != nil {
		t.Fatalf("CloseOrder: %v", err)
	}
	if fp.closeGot == nil || fp.closeGot.OutTradeNo != "OUT-9" {
		t.Errorf("OutTradeNo not propagated: %+v", fp.closeGot)
	}
}

func TestCloseOrder_ErrorIsTransparent(t *testing.T) {
	sentinel := errors.New("upstream 5xx")
	fp := &fakeProvider{channel: fakeChannel, closeErr: sentinel}
	g := newTestGateway(t, fp)

	err := g.CloseOrder(context.Background(), orderflow.Channel(fakeChannel), "OUT-9")
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
}

// ----- QueryOrder -----

func TestQueryOrder_SuccessMapsFields(t *testing.T) {
	paidAt := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	fp := &fakeProvider{
		channel: fakeChannel,
		queryResp: &paymgr.QueryOrderResponse{
			Channel:       fakeChannel,
			OutTradeNo:    "OUT-10",
			TransactionID: "TXN-ABC",
			TradeStatus:   paymgr.TradeStatus("paid"),
			TotalAmount:   9900,
			PaidAt:        paidAt,
		},
	}
	g := newTestGateway(t, fp)

	res, err := g.QueryOrder(context.Background(), orderflow.Channel(fakeChannel), "OUT-10")
	if err != nil {
		t.Fatalf("QueryOrder: %v", err)
	}

	if fp.queryGot == nil || fp.queryGot.OutTradeNo != "OUT-10" {
		t.Errorf("OutTradeNo not propagated to Provider: %+v", fp.queryGot)
	}
	if res.OutTradeNo != "OUT-10" || res.TransactionID != "TXN-ABC" {
		t.Errorf("trade identifiers mismatch: %+v", res)
	}
	if res.TradeStatus != orderflow.TradeStatusPaid {
		t.Errorf("TradeStatus = %q, want paid", res.TradeStatus)
	}
	if res.TotalAmount != 9900 {
		t.Errorf("TotalAmount = %d", res.TotalAmount)
	}
	if !res.PaidAt.Equal(paidAt) {
		t.Errorf("PaidAt mismatch: got %v want %v", res.PaidAt, paidAt)
	}
	if res.Channel != orderflow.Channel(fakeChannel) {
		t.Errorf("Channel = %q", res.Channel)
	}
}

func TestQueryOrder_ErrorIsTransparent(t *testing.T) {
	sentinel := errors.New("gateway timeout")
	fp := &fakeProvider{channel: fakeChannel, queryErr: sentinel}
	g := newTestGateway(t, fp)

	res, err := g.QueryOrder(context.Background(), orderflow.Channel(fakeChannel), "OUT-X")
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
	if (res != orderflow.QueryResult{}) {
		t.Errorf("result should be zero on error, got %+v", res)
	}
}

// ----- ParseNotify -----

func TestParseNotify_SuccessMapsFieldsAndPreservesRaw(t *testing.T) {
	paidAt := time.Date(2026, 4, 22, 13, 30, 0, 0, time.UTC)
	fp := &fakeProvider{
		channel: fakeChannel,
		notifyResp: &paymgr.NotifyResult{
			Channel:       fakeChannel,
			OutTradeNo:    "OUT-N",
			TransactionID: "TXN-N",
			TradeStatus:   paymgr.TradeStatus("paid"),
			TotalAmount:   100,
			PaidAt:        paidAt,
			BuyerID:       "buyer-1",
			Metadata:      map[string]string{"order_token": "tok-N"},
		},
	}
	g := newTestGateway(t, fp)

	req := httptest.NewRequest(http.MethodPost, "/notify", nil)
	res, err := g.ParseNotify(context.Background(), orderflow.Channel(fakeChannel), req)
	if err != nil {
		t.Fatalf("ParseNotify: %v", err)
	}
	if res.OutTradeNo != "OUT-N" || res.TransactionID != "TXN-N" {
		t.Errorf("trade identifiers mismatch: %+v", res)
	}
	if res.TradeStatus != orderflow.TradeStatusPaid {
		t.Errorf("TradeStatus = %q", res.TradeStatus)
	}
	if res.TotalAmount != 100 || !res.PaidAt.Equal(paidAt) {
		t.Errorf("amount/paidAt mismatch: %+v", res)
	}
	if res.Channel != orderflow.Channel(fakeChannel) {
		t.Errorf("Channel = %q", res.Channel)
	}
	// Raw 必须是 Provider 返回的原始指针，业务侧可以靠它拿到 BuyerID / Metadata 等扩展字段
	rawNotify, ok := res.Raw.(*paymgr.NotifyResult)
	if !ok || rawNotify != fp.notifyResp {
		t.Fatalf("Raw should preserve the original *paymgr.NotifyResult pointer")
	}
	if rawNotify.BuyerID != "buyer-1" {
		t.Errorf("BuyerID lost: %+v", rawNotify)
	}
}

func TestParseNotify_SignatureFailureIsTransparent(t *testing.T) {
	sentinel := errors.New("signature verification failed")
	fp := &fakeProvider{channel: fakeChannel, notifyErr: sentinel}
	g := newTestGateway(t, fp)

	req := httptest.NewRequest(http.MethodPost, "/notify", nil)
	res, err := g.ParseNotify(context.Background(), orderflow.Channel(fakeChannel), req)
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
	// 验签失败时必须返回零值 NotifyResult——Engine 的安全契约依赖这一点
	if res.OutTradeNo != "" || res.TradeStatus != "" || res.Raw != nil {
		t.Errorf("result must be zero on signature failure, got %+v", res)
	}
}

// ----- AckNotify -----

func TestAckNotify_Success(t *testing.T) {
	fp := &fakeProvider{channel: fakeChannel}
	g := newTestGateway(t, fp)

	rec := httptest.NewRecorder()
	if err := g.AckNotify(orderflow.Channel(fakeChannel), rec); err != nil {
		t.Fatalf("AckNotify: %v", err)
	}
	if !fp.ackCalled {
		t.Error("Provider.ACKNotify was not invoked")
	}
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Errorf("response not propagated: code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestAckNotify_UnknownChannelReturnsError(t *testing.T) {
	fp := &fakeProvider{channel: fakeChannel}
	g := newTestGateway(t, fp)

	rec := httptest.NewRecorder()
	err := g.AckNotify(orderflow.Channel("not-registered"), rec)
	if err == nil {
		t.Error("expected error for unregistered channel")
	}
}
