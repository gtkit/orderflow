package orderflow

// PayMethod 表示订单的支付方式。
//
// 数值与下游业务参考标准对齐：
//
//	0 未选择（零值，下单时尚未选定支付方式）
//	1 微信支付
//	2 支付宝支付
//	3 银联支付
//
// 业务方在持久化时按 int8 存储；orderflow Engine 不解读其语义，
// 仅用于在 OrderSpec / OrderSnapshot 间透传。
type PayMethod int8

const (
	// PayMethodWechat 微信支付。
	PayMethodWechat PayMethod = 1
	// PayMethodAlipay 支付宝支付。
	PayMethodAlipay PayMethod = 2
	// PayMethodUnion 银联支付。
	PayMethodUnion PayMethod = 3
)

// String 返回支付方式的中文名称，供日志 / 调试使用。
func (p PayMethod) String() string {
	switch p {
	case 0:
		return "未选择"
	case PayMethodWechat:
		return "微信支付"
	case PayMethodAlipay:
		return "支付宝支付"
	case PayMethodUnion:
		return "银联支付"
	default:
		return "未知"
	}
}
