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
//
// 返回的 RefundResponse.Status 按渠道行为契约填写：
//   - 支付宝：同步成功即终态——填 RefundTradeStatusSucceeded，调用方可立即触发反向核销
//   - 微信：同步成功是中间态——填 RefundTradeStatusProcessing，调用方应等异步通知
//   - 其他 / 未识别渠道：保守填 RefundTradeStatusProcessing，让调用方走 Query / 异步通知路径
func (g *Gateway) Refund(ctx context.Context, ch orderflow.Channel, req orderflow.RefundRequest) (orderflow.RefundResponse, error) {
	if err := g.Validate(); err != nil {
		return orderflow.RefundResponse{}, err
	}
	resp, err := g.mgr.Refund(ctx, paymgr.Channel(ch), &paymgr.RefundRequest{
		OutTradeNo:    req.OutTradeNo,
		TransactionID: req.TransactionID,
		OutRefundNo:   req.OutRefundNo,
		RefundAmount:  req.RefundAmount,
		TotalAmount:   req.TotalAmount,
		Reason:        req.Reason,
		NotifyURL:     req.NotifyURL,
	})
	if err != nil {
		return orderflow.RefundResponse{}, err
	}
	return orderflow.RefundResponse{
		OutRefundNo:     resp.OutRefundNo,
		GatewayRefundID: resp.RefundID,
		Status:          syncRefundStatus(paymgr.Channel(ch)),
		RefundAmount:    resp.RefundAmount,
		Channel:         orderflow.Channel(resp.Channel),
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
		OutTradeNo:      resp.OutTradeNo,
		TransactionID:   resp.TransactionID,
		GatewayRefundID: resp.RefundID,
		Status:          mapRefundStatus(resp.RefundStatus),
		RefundAmount:    resp.RefundAmount,
		TotalAmount:     resp.TotalAmount,
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
		OutRefundNo:         n.OutRefundNo,
		OutTradeNo:          n.OutTradeNo,
		TransactionID:       n.TransactionID,
		GatewayRefundID:     n.RefundID,
		Status:              mapRefundStatus(n.RefundStatus),
		RefundAmount:        n.RefundAmount,
		TotalAmount:         n.TotalAmount,
		SucceededAt:         n.RefundedAt,
		Channel:             orderflow.Channel(n.Channel),
		UserReceivedAccount: n.UserReceivedAccount,
		Raw:                 n,
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
// 已识别的常见错误码（基于阅读 paymgr 源码 + 经验，未在生产环境穷举验证）：
//
//	微信：RESOURCE_ALREADY_EXISTS（退款单已存在）/ DUPLICATE_REQUEST（重复请求）
//	支付宝：ACQ.DUPLICATE_REFUND_REQUEST（重复退款请求）/ ACQ.TRADE_HAS_REFUND_LIMIT（金额一致幂等）
//
// 业务方在生产环境观察到本函数未识别的渠道幂等错误码（典型表现：调 Refund
// 拿到错误 → IsIgnorableRefundError 返回 false → 但 QueryRefund 显示渠道侧已存在
// 该退款单），请提交 PR 补充本函数；业务侧在收到补丁前可在自己的编排里加一层
// "未识别错误时主动 Query 兜底"逻辑作为临时缓解。
//
// nil error 返回 false。
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

// syncRefundStatus 按"已知渠道行为模式"启发式决定 RefundResponse.Status。
//
//   - 支付宝：同步成功通常 = 终态 succeeded（渠道侧立即完成退款）
//   - 微信：同步成功通常 = processing（渠道侧后续异步处理，等异步通知或 Query）
//   - 其他 / 未识别渠道：保守填 processing，业务方按 Query / 异步通知路径推进
//
// **重要：本函数返回值是启发式默认值，不是确定性映射**。底层 paymgr.RefundResponse
// 当前不暴露原始 status 字段，本函数只能按渠道历史行为约定填写。渠道行为发生变化
// 时（如支付宝引入风控异步审查、微信调整退款流程），返回值可能与真实状态偏差。
//
// 业务方使用约束：
//
//  1. **必须用 `Status.IsTerminal()` 判断**而不是按渠道名硬编码业务分支，确保未来
//     渠道行为变化或新增渠道时业务代码保持正确
//  2. 对状态准确性敏感的场景应调 QueryRefund 主动对账，不完全信任本字段
//  3. 业务方观察到本函数与真实渠道行为偏差时应提交 PR 修正
func syncRefundStatus(ch paymgr.Channel) orderflow.RefundTradeStatus {
	switch ch {
	case paymgr.ChannelAlipay:
		return orderflow.RefundTradeStatusSucceeded
	case paymgr.ChannelWechat:
		return orderflow.RefundTradeStatusProcessing
	default:
		return orderflow.RefundTradeStatusProcessing
	}
}

// mapRefundStatus 把底层 paymgr.RefundStatus 映射到 orderflow.RefundTradeStatus。
//
// 映射规则：
//
//	processing → Processing（中间态，等推进）
//	success    → Succeeded（终态，触发反向核销）
//	closed     → Failed（终态失败：退款关闭未成功）
//	error      → Failed（终态失败：未知错误）
//	abnormal   → Unknown（**非终态**——paymgr 注释明确"需人工介入"，
//	             业务方应告警 + 不触发反向核销 + 等人工处理后渠道推进到真正终态）
//	未识别字面量 → Unknown（让业务方告警识别 SDK 版本升级 / 新渠道行为）
//
// 业务方需要区分 abnormal vs 未识别字面量时通过 Raw 字段类型断言取回原始 paymgr.RefundStatus。
func mapRefundStatus(s paymgr.RefundStatus) orderflow.RefundTradeStatus {
	switch s {
	case paymgr.RefundStatusProcessing:
		return orderflow.RefundTradeStatusProcessing
	case paymgr.RefundStatusSuccess:
		return orderflow.RefundTradeStatusSucceeded
	case paymgr.RefundStatusClosed, paymgr.RefundStatusError:
		return orderflow.RefundTradeStatusFailed
	case paymgr.RefundStatusAbnormal:
		// 需人工介入——非终态，业务方应告警 + 等人工推进
		return orderflow.RefundTradeStatusUnknown
	default:
		// driver 暂未识别的状态字面量——让业务方告警感知 SDK 升级或新渠道行为
		return orderflow.RefundTradeStatusUnknown
	}
}
