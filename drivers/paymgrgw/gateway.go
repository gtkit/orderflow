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
//
// # 幂等契约满足度（当前实现状态）
//
// 核心包 orderflow.PaymentGateway.UnifiedOrder 要求实现方以 OutTradeNo 为幂等键，
// 把"订单已存在且未支付"识别为成功响应。本 driver **当前依赖上游 paymgr / go-pay
// 透传到底层网关的天然幂等性**：
//
//   - 微信 V3 transactions/jsapi：相同 out_trade_no + 相同金额返回原 prepay_id
//     等价的成功响应（go-pay 默认透传成功）。
//   - 支付宝 OpenAPI：相同 out_trade_no + 相同金额返回原 trade_no（go-pay 透传成功）。
//
// 这两条主流路径下，本 driver 直接透传 paymgr.UnifiedOrder 错误是合规的——
// 上游不会把"订单已存在且未支付"作为错误返回。
//
// **未来风险点**：若接入新渠道（jdpay / unipay / 自研沙箱等）返回独有的"订单已存在"
// 错误码，需要在此函数内补 errors.As(&paymgr.ChannelError{}) + code 翻译逻辑，
// 把它转成 QueryOrder 拉取的等价 UnifiedOrderResponse 返回。
//
// paymgr.ErrOrderPaid / paymgr.ErrOrderClosed 这两种 sentinel 错误**不在**翻译范围内：
// 它们意味着订单已经走到不可重试的终态，Engine 应让错误浮上去由调用方处理。
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
