package orderflow

import (
	"context"
	"net/http"
)

// RefundGateway 抽象了退款发起、查询、异步通知解析与回写等退款侧操作。
//
// 与 PaymentGateway 并列存在，让一个 driver 实例可以同时实现两个接口（典型如
// drivers/paymgrgw 的 *Gateway），调用方在支付服务和退款服务两条路径上可复用同一实例。
//
// # 协议层定位（接入方必读）
//
// 本接口只屏蔽渠道协议层的差异（微信 / 支付宝等 SDK 类型 / 错误码），**不**做退款流程的
// 业务编排。退款的审批工作流、金额计算（手续费 / 按使用比例 / 补偿券折抵）、退款记录持久化、
// 反向核销策略均由调用方自行实现——库内不引入 Refunder facade、不扩展 Store / OrderSnapshot
// 接口、不提供退款相关钩子或观察事件。
//
// 调用方典型编排（详见主仓 README "退款（自行编排）" 章节）：
//
//  1. 业务侧审批通过，得到最终退款金额；
//  2. 业务侧事务内 INSERT 退款记录（status = pending）；
//  3. 调 Gateway.Refund(ctx, ch, req)（事务外）；
//  4. 视返回 / IsIgnorableRefundError 决定走 CAS 落 succeeded/failed 或走 QueryRefund 兜底；
//  5. 异步通知路径：ParseRefundNotify → CAS UPDATE（含 status NOT IN ('succeeded','failed') 防终态被覆盖）→ AckRefundNotify。
//
// # 安全契约（实现方必须遵守）
//
// **ParseRefundNotify 必须完成签名验证后才能返回成功的 RefundNotifyResult**。调用方
// 信任 ParseRefundNotify 的输出已经经过验签，不会做二次验签。如果 driver 忽略验签
// （例如直接 json.Unmarshal HTTP body），攻击者可以伪造 `{"refund_status":"success",
// "refund_amount":1,...}` 的 POST 请求让调用方的 CAS 推进到 succeeded 并触发反向核销。
// 这是关键的"默认安全"防线，实现方跳过即视为破坏合约。
//
// 验签失败时 ParseRefundNotify 必须返回 error（**不是** 返回零值或 RefundTradeStatusFailed
// 的 RefundNotifyResult）。
//
// **Refund / QueryRefund** 的实现应自身完成网关通信的 TLS + 凭证管理。
//
// **driver 实装方**应在自己的源文件中加 `var _ orderflow.RefundGateway = (*MyDriver)(nil)`
// 编译期类型断言，避免接口漂移导致运行时陷阱。
type RefundGateway interface {
	// Refund 在支付网关侧发起退款。
	//
	// 同一个 OutRefundNo 多次调用，渠道侧通常返回"退款单已存在 / 已成功 / 金额一致"
	// 类幂等错误——driver 必须通过 IsIgnorableRefundError 把这类错误映射为 true，
	// 让调用方走 QueryRefund 路径拿真实状态推进编排，不重复创建退款单。
	Refund(ctx context.Context, ch Channel, req RefundRequest) (RefundResponse, error)

	// QueryRefund 按商户退款单号查询退款状态。
	//
	// 用于：(1) 主动对账；(2) Refund 调用得到 IsIgnorableRefundError == true 时
	// 拉取真实状态推进编排；(3) 长时间 pending 的退款诊断漂移。
	//
	// 退款记录在网关侧不存在时，driver 应返回 ErrRefundNotFound（或 errors.Is 可识别的衍生错误），
	// 让调用方据此走"DB 已留下 pending 但渠道侧从未受理"的兜底路径。
	QueryRefund(ctx context.Context, ch Channel, outRefundNo string) (RefundQueryResult, error)

	// ParseRefundNotify 解析并验签退款异步通知请求。
	//
	// 实现方必须完成验签后才能返回成功的 RefundNotifyResult；验签失败时必须返回 error
	// 而不是构造伪造的 result（详见接口级别的"安全契约"段落）。
	//
	// 部分渠道（如支付宝）的退款结果通过支付通知端点回调，driver 应内部识别后映射；
	// 调用方只需为退款单独配置一个 endpoint 调本方法即可。
	ParseRefundNotify(ctx context.Context, ch Channel, r *http.Request) (RefundNotifyResult, error)

	// AckRefundNotify 向支付网关回写"已收到退款通知"的成功响应。
	//
	// 调用方必须在 ParseRefundNotify 成功 + 业务侧落库后调用本方法，否则渠道会持续重发通知。
	// 业务侧落库失败时**不**应调本方法，让渠道重发让本侧再次尝试。
	AckRefundNotify(ch Channel, w http.ResponseWriter) error

	// IsIgnorableRefundError 判断 Refund 返回的错误是否属于"渠道侧已处理过该退款单"
	// 类幂等错误，调用方据此走 QueryRefund 路径拿真实状态而不是向上抛错。
	//
	// 可忽略（返回 true）的典型场景：
	//   - 退款单已存在（同一 OutRefundNo 二次提交）
	//   - 退款单已成功（重复发起已完成的退款）
	//   - 退款金额一致幂等成功
	//
	// 不可忽略（返回 false）的典型场景：
	//   - 网络超时 / 5xx
	//   - 签名错误
	//   - 余额不足
	//   - 订单不存在 / 订单未支付
	//   - 退款金额不合法（> 原金额）
	//   - 任何其他业务级错误
	//
	// nil error 时必须返回 false。
	IsIgnorableRefundError(ch Channel, err error) bool
}
