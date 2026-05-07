// Package paymgrgw 把 github.com/gtkit/go-pay/paymgr 的 Manager 适配为 orderflow.PaymentGateway
// 与 orderflow.RefundGateway。同一个 *Gateway 实例同时实现两个接口（v1.2.0+），
// 业务方拿同一指针既可传给支付路径（Engine.Config.Gateway）也可传给退款路径
// （调用方自行编排的 RefundGateway 注入位）。
//
// 使用示例：
//
//	import (
//	    "github.com/gtkit/go-pay/paymgr"
//	    "github.com/gtkit/orderflow"
//	    "github.com/gtkit/orderflow/drivers/paymgrgw"
//	)
//
//	gateway := paymgrgw.New(paymgr.NewManager())
//	engine, err := orderflow.New[MyOrder](orderflow.Config[MyOrder]{
//	    Gateway: gateway,
//	    // ...其他能力接口
//	})
//	// 同一实例在退款服务中作为 RefundGateway 注入：
//	var _ orderflow.RefundGateway = gateway
//
// 若下单场景需要覆盖默认的 TradeTypeApp（例如 H5 / JSAPI），使用 WithTradeType：
//
//	gateway := paymgrgw.New(mgr, paymgrgw.WithTradeType(paymgr.TradeTypeH5))
//
// 退款流程的协议层契约见核心包 refund_gateway.go；调用方自行编排的指南见主仓 README
// "退款（自行编排）" 章节。
package paymgrgw
