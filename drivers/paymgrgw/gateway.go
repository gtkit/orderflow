package paymgrgw

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gtkit/go-pay/paymgr"
	"github.com/gtkit/orderflow"
)

// Gateway 把 *paymgr.Manager 适配为 orderflow.PaymentGateway。
type Gateway struct {
	mgr       *paymgr.Manager
	tradeType paymgr.TradeType
}

// Option 用于可选配置。
type Option func(*Gateway)

// WithTradeType 覆盖默认的 TradeTypeApp。
// 例如：H5 下单用 WithTradeType(paymgr.TradeTypeH5)；JSAPI 用 TradeTypeJsapi。
func WithTradeType(t paymgr.TradeType) Option {
	return func(g *Gateway) { g.tradeType = t }
}

// New 构造 Gateway。mgr 必填；不传 Option 时 TradeType 默认为 TradeTypeApp。
func New(mgr *paymgr.Manager, opts ...Option) *Gateway {
	g := &Gateway{
		mgr:       mgr,
		tradeType: paymgr.TradeTypeApp,
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

var _ orderflow.PaymentGateway = (*Gateway)(nil)

// Validate reports whether the gateway has all required internal dependencies.
func (g *Gateway) Validate() error {
	if g == nil {
		return fmt.Errorf("paymgrgw: gateway is nil")
	}
	if g.mgr == nil {
		return fmt.Errorf("paymgrgw: manager is nil")
	}
	return nil
}

// UnifiedOrder 下单并返回客户端拉起支付所需参数。
func (g *Gateway) UnifiedOrder(ctx context.Context, ch orderflow.Channel, req orderflow.UnifiedOrderRequest) (orderflow.UnifiedOrderResponse, error) {
	if err := g.Validate(); err != nil {
		return orderflow.UnifiedOrderResponse{}, err
	}
	resp, err := g.mgr.UnifiedOrder(ctx, paymgr.Channel(ch), &paymgr.UnifiedOrderRequest{
		OutTradeNo:  req.OutTradeNo,
		TotalAmount: req.TotalAmount,
		Subject:     req.Subject,
		TradeType:   g.tradeType,
		NotifyURL:   req.NotifyURL,
		ExpireAt:    req.ExpireAt,
		Metadata:    req.Metadata,
	})
	if err != nil {
		return orderflow.UnifiedOrderResponse{}, err
	}
	return orderflow.UnifiedOrderResponse{
		AppParams: resp.AppParams,
		Raw:       resp,
	}, nil
}

// CloseOrder 关闭网关侧订单。
func (g *Gateway) CloseOrder(ctx context.Context, ch orderflow.Channel, orderNo string) error {
	if err := g.Validate(); err != nil {
		return err
	}
	return g.mgr.CloseOrder(ctx, paymgr.Channel(ch), &paymgr.CloseOrderRequest{OutTradeNo: orderNo})
}

// QueryOrder 向网关查询订单真实状态，用于对账与恢复。
func (g *Gateway) QueryOrder(ctx context.Context, ch orderflow.Channel, orderNo string) (orderflow.QueryResult, error) {
	if err := g.Validate(); err != nil {
		return orderflow.QueryResult{}, err
	}
	resp, err := g.mgr.QueryOrder(ctx, paymgr.Channel(ch), &paymgr.QueryOrderRequest{OutTradeNo: orderNo})
	if err != nil {
		return orderflow.QueryResult{}, err
	}
	return orderflow.QueryResult{
		OutTradeNo:    resp.OutTradeNo,
		TransactionID: resp.TransactionID,
		TradeStatus:   orderflow.TradeStatus(resp.TradeStatus),
		TotalAmount:   resp.TotalAmount,
		PaidAt:        resp.PaidAt,
		Channel:       orderflow.Channel(resp.Channel),
	}, nil
}

// ParseNotify 解析并验签支付回调。
func (g *Gateway) ParseNotify(ctx context.Context, ch orderflow.Channel, r *http.Request) (orderflow.NotifyResult, error) {
	if err := g.Validate(); err != nil {
		return orderflow.NotifyResult{}, err
	}
	n, err := g.mgr.ParseNotify(ctx, paymgr.Channel(ch), r)
	if err != nil {
		return orderflow.NotifyResult{}, err
	}
	return orderflow.NotifyResult{
		OutTradeNo:    n.OutTradeNo,
		TransactionID: n.TransactionID,
		TradeStatus:   orderflow.TradeStatus(n.TradeStatus),
		TotalAmount:   n.TotalAmount,
		PaidAt:        n.PaidAt,
		Channel:       orderflow.Channel(n.Channel),
		Raw:           n,
	}, nil
}

// AckNotify 向网关回写成功响应。
func (g *Gateway) AckNotify(ch orderflow.Channel, w http.ResponseWriter) error {
	if err := g.Validate(); err != nil {
		return err
	}
	return g.mgr.ACKNotify(paymgr.Channel(ch), w)
}

// IsIgnorableCloseError 判断网关 Close 返回的错误是否可忽略（订单不存在 / 已关闭）。
// 通用判定：ErrOrderNotFound / ErrOrderClosed；
// 支付宝额外兼容 ACQ.TRADE_NOT_EXIST 这种未创建到网关侧的订单关闭场景。
func (g *Gateway) IsIgnorableCloseError(ch orderflow.Channel, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, paymgr.ErrOrderNotFound) || errors.Is(err, paymgr.ErrOrderClosed) {
		return true
	}

	var channelErr *paymgr.ChannelError
	if !errors.As(err, &channelErr) {
		return false
	}
	if paymgr.Channel(ch) == paymgr.ChannelAlipay {
		return channelErr.Code == "ACQ.TRADE_NOT_EXIST"
	}
	return false
}
