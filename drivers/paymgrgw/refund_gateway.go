package paymgrgw

import (
	"context"
	"errors"
	"net/http"

	"github.com/gtkit/go-pay/paymgr"
	"github.com/gtkit/orderflow"
)

var _ orderflow.RefundGateway = (*Gateway)(nil)

// Refund 在网关侧发起退款。
//
// 同一 OutRefundNo 重复提交时，渠道侧通常返回"退款单已存在 / 已成功"类幂等错误，
// 由 IsIgnorableRefundError 映射为可忽略，调用方据此走 QueryRefund 路径拿真实状态。
func (g *Gateway) Refund(ctx context.Context, ch orderflow.Channel, req orderflow.RefundRequest) (orderflow.RefundResponse, error) {
	if err := g.Validate(); err != nil {
		return orderflow.RefundResponse{}, err
	}
	resp, err := g.mgr.Refund(ctx, paymgr.Channel(ch), &paymgr.RefundRequest{
		OutTradeNo:   req.OutTradeNo,
		OutRefundNo:  req.OutRefundNo,
		RefundAmount: req.RefundAmount,
		TotalAmount:  req.TotalAmount,
		Reason:       req.Reason,
		NotifyURL:    req.NotifyURL,
	})
	if err != nil {
		return orderflow.RefundResponse{}, err
	}
	return orderflow.RefundResponse{
		OutRefundNo:     resp.OutRefundNo,
		GatewayRefundID: resp.RefundID,
		Raw:             resp,
	}, nil
}

// QueryRefund 按商户退款单号查询退款状态。
//
// 网关侧找不到退款单时统一返回 orderflow.ErrRefundNotFound，调用方可用 errors.Is 识别。
func (g *Gateway) QueryRefund(ctx context.Context, ch orderflow.Channel, outRefundNo string) (orderflow.RefundQueryResult, error) {
	if err := g.Validate(); err != nil {
		return orderflow.RefundQueryResult{}, err
	}
	resp, err := g.mgr.QueryRefund(ctx, paymgr.Channel(ch), &paymgr.QueryRefundRequest{
		OutRefundNo: outRefundNo,
	})
	if err != nil {
		if isRefundNotFound(ch, err) {
			return orderflow.RefundQueryResult{}, orderflow.ErrRefundNotFound
		}
		return orderflow.RefundQueryResult{}, err
	}
	return orderflow.RefundQueryResult{
		OutRefundNo:     resp.OutRefundNo,
		GatewayRefundID: resp.RefundID,
		Status:          mapRefundStatus(resp.RefundStatus),
		RefundAmount:    resp.RefundAmount,
		SucceededAt:     resp.RefundedAt,
		Channel:         orderflow.Channel(resp.Channel),
		Raw:             resp,
	}, nil
}

// ParseRefundNotify 解析并验签退款异步通知。
//
// 验签失败时返回 error，**不**返回伪造的零值或失败结果——这是 RefundGateway 的安全契约，
// 与 PaymentGateway.ParseNotify 同源，调用方据此信任返回的 RefundNotifyResult 已经验签。
func (g *Gateway) ParseRefundNotify(ctx context.Context, ch orderflow.Channel, r *http.Request) (orderflow.RefundNotifyResult, error) {
	if err := g.Validate(); err != nil {
		return orderflow.RefundNotifyResult{}, err
	}
	n, err := g.mgr.ParseRefundNotify(ctx, paymgr.Channel(ch), r)
	if err != nil {
		return orderflow.RefundNotifyResult{}, err
	}
	return orderflow.RefundNotifyResult{
		OutRefundNo:     n.OutRefundNo,
		GatewayRefundID: n.RefundID,
		Status:          mapRefundStatus(n.RefundStatus),
		RefundAmount:    n.RefundAmount,
		SucceededAt:     n.RefundedAt,
		Channel:         orderflow.Channel(n.Channel),
		Raw:             n,
	}, nil
}

// AckRefundNotify 向网关回写"已收到退款通知"的成功响应。
//
// 底层 paymgr.Manager.ACKNotify 同时承担支付通知与退款通知的 ack 写入——同一渠道下
// 两条管道的 ack 协议本身相同（微信 XML / 支付宝 plain text），无需独立方法。
func (g *Gateway) AckRefundNotify(ch orderflow.Channel, w http.ResponseWriter) error {
	if err := g.Validate(); err != nil {
		return err
	}
	return g.mgr.ACKNotify(paymgr.Channel(ch), w)
}

// IsIgnorableRefundError 判断 Refund 返回的错误是否属于"渠道侧已处理过该退款单"
// 类幂等错误。返回 true 时，调用方应走 QueryRefund 路径拿真实状态。
//
// 已识别的可忽略错误码：
//
//	微信：RESOURCE_ALREADY_EXISTS（退款单已存在）/ DUPLICATE_REQUEST（重复请求）
//	支付宝：ACQ.DUPLICATE_REFUND_REQUEST（重复退款请求）/ ACQ.TRADE_HAS_REFUND_LIMIT（金额一致幂等）
//
// 业务方观察到新的渠道幂等错误码需要纳入识别时，请提交 PR 补充本函数。
func (g *Gateway) IsIgnorableRefundError(ch orderflow.Channel, err error) bool {
	if err == nil {
		return false
	}

	var channelErr *paymgr.ChannelError
	if !errors.As(err, &channelErr) {
		return false
	}

	switch paymgr.Channel(ch) {
	case paymgr.ChannelWechat:
		switch channelErr.Code {
		case "RESOURCE_ALREADY_EXISTS", "DUPLICATE_REQUEST":
			return true
		}
	case paymgr.ChannelAlipay:
		switch channelErr.Code {
		case "ACQ.DUPLICATE_REFUND_REQUEST", "ACQ.TRADE_HAS_REFUND_LIMIT":
			return true
		}
	}
	return false
}

// isRefundNotFound 判断错误是否表示"退款单不存在"。
//
//	支付宝：底层在 ACQ.TRADE_NOT_EXIST 路径上直接返回 paymgr.ErrOrderNotFound。
//	微信：底层未单独映射，通过 ChannelError.Code == "RESOURCE_NOT_EXISTS" 识别。
func isRefundNotFound(ch orderflow.Channel, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, paymgr.ErrOrderNotFound) {
		return true
	}
	var channelErr *paymgr.ChannelError
	if !errors.As(err, &channelErr) {
		return false
	}
	if paymgr.Channel(ch) == paymgr.ChannelWechat && channelErr.Code == "RESOURCE_NOT_EXISTS" {
		return true
	}
	return false
}

// mapRefundStatus 把底层 paymgr.RefundStatus 映射到 orderflow.RefundTradeStatus。
//
// 映射规则按 design.md D3：closed / abnormal / error 全部归为 Failed（终态失败）；
// 业务方需要区分 abnormal（人工介入）时通过 Raw 字段类型断言取回原始 paymgr.RefundStatus。
func mapRefundStatus(s paymgr.RefundStatus) orderflow.RefundTradeStatus {
	switch s {
	case paymgr.RefundStatusProcessing:
		return orderflow.RefundTradeStatusProcessing
	case paymgr.RefundStatusSuccess:
		return orderflow.RefundTradeStatusSucceeded
	case paymgr.RefundStatusClosed,
		paymgr.RefundStatusAbnormal,
		paymgr.RefundStatusError:
		return orderflow.RefundTradeStatusFailed
	default:
		// 渠道返回了 driver 暂未识别的状态字面量——回退到 pending 让调用方继续观察 / 重试 Query，
		// 不直接映射为 Failed 避免误触发反向核销终态。
		return orderflow.RefundTradeStatusPending
	}
}
