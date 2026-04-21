// Package paymgrgw 把 github.com/gtkit/go-pay/paymgr 的 Manager 适配为 orderflow.PaymentGateway。
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
//
// 若下单场景需要覆盖默认的 TradeTypeApp（例如 H5 / JSAPI），使用 WithTradeType：
//
//	gateway := paymgrgw.New(mgr, paymgrgw.WithTradeType(paymgr.TradeTypeH5))
package paymgrgw
