package orderflow_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gtkit/orderflow"
)

// ExampleOrderStatus_String 演示状态枚举的语义字符串，常用于日志 / 监控标签。
func ExampleOrderStatus_String() {
	fmt.Println(orderflow.StatusPending.String())
	fmt.Println(orderflow.StatusPaid.String())
	fmt.Println(orderflow.StatusDelivered.String())
	fmt.Println(orderflow.StatusClosed.String())
	// Output:
	// pending
	// paid
	// delivered
	// closed
}

// ExampleOrderStatus_IsTerminal 演示终态判定——常用于判断"能否继续跃迁"或"SSE 应不应该关闭连接"。
func ExampleOrderStatus_IsTerminal() {
	fmt.Println(orderflow.StatusPending.IsTerminal())   // 可跃迁
	fmt.Println(orderflow.StatusPaid.IsTerminal())      // 可跃迁到 Delivered
	fmt.Println(orderflow.StatusDelivered.IsTerminal()) // 还能到 Completed
	fmt.Println(orderflow.StatusCompleted.IsTerminal()) // 终态
	fmt.Println(orderflow.StatusClosed.IsTerminal())    // 终态
	fmt.Println(orderflow.StatusCancelled.IsTerminal()) // 终态
	// Output:
	// false
	// false
	// false
	// true
	// true
	// true
}

// ExampleOrderStatus_CanTransitionTo 演示状态跃迁白名单，
// Engine 内部所有 CAS 都基于此表，业务方需要做后台管理/修正时也应遵守。
//
// 注意：Closed → Paid 不在此表内——那条路径走专用 CASReopenPaid
// （网关确认已付的恢复场景），见 engine_notify.go 的 handleClosedPaidNotify。
func ExampleOrderStatus_CanTransitionTo() {
	fmt.Println(orderflow.StatusPending.CanTransitionTo(orderflow.StatusPaid))
	fmt.Println(orderflow.StatusPending.CanTransitionTo(orderflow.StatusClosed))
	fmt.Println(orderflow.StatusPaid.CanTransitionTo(orderflow.StatusDelivered))
	fmt.Println(orderflow.StatusClosed.CanTransitionTo(orderflow.StatusPaid))
	// Output:
	// true
	// true
	// true
	// false
}

// Example_errorHandling 演示如何用 errors.Is 处理 Engine 返回的 sentinel 错误。
// 业务 API 层应把这些错误翻译成对应的 HTTP 状态码。
func Example_errorHandling() {
	// 假设上游业务调用 engine.PollStatus 得到一个错误
	err := fmt.Errorf("orderflow query: %w", orderflow.ErrOrderForbidden)

	switch {
	case errors.Is(err, orderflow.ErrOrderNotFound):
		fmt.Println("respond 404")
	case errors.Is(err, orderflow.ErrOrderForbidden):
		fmt.Println("respond 403")
	case errors.Is(err, orderflow.ErrOrderExpired):
		fmt.Println("respond 410")
	case errors.Is(err, orderflow.ErrConcurrentCreate):
		fmt.Println("respond 429 (too many requests)")
	default:
		fmt.Println("respond 500")
	}
	// Output:
	// respond 403
}

// ExampleClosedReason 演示订单关闭原因枚举在 OnClosed 钩子中的用法。
func ExampleClosedReason() {
	reason := orderflow.ClosedReasonTimeout
	switch reason {
	case orderflow.ClosedReasonTimeout:
		fmt.Println("订单支付超时")
	case orderflow.ClosedReasonSuperseded:
		fmt.Println("被同用户的新订单替代")
	case orderflow.ClosedReasonManual:
		fmt.Println("管理员/用户主动关闭")
	case orderflow.ClosedReasonEnqueueFail:
		fmt.Println("延时队列入队失败，自我保护关闭")
	}
	// Output:
	// 订单支付超时
}

// ----- 下面是依赖 Engine 的文档型 Example：
//   展示调用形式和错误处理模式，供 godoc 可见。
//   由于 Engine 依赖完整的 Store / Gateway / DelayQueue / Cache / Stream，
//   构造成本过高，这些 Example 不带 // Output 断言——go test 会编译验证它们。

// ExampleEngine_Create 演示创建订单的标准调用形式。
// userID 必须来自鉴权上下文，不得从 HTTP body / query 读取。
func ExampleEngine_Create() {
	var engine *orderflow.Engine[*orderSnapshot] // 由 orderflow.New(...) 构造
	ctx := context.Background()

	result, err := engine.Create(ctx, orderflow.CreateRequest{
		UserID:    1001, // 必须来自鉴权上下文
		PayMethod: "wechat_app",
		ClientIP:  "203.0.113.10",
		Product: orderflow.ProductInfo{
			ID:    42,
			Type:  "vip",
			Title: "VIP 月卡",
			Price: 9900, // 单位：分
		},
	})
	switch {
	case errors.Is(err, orderflow.ErrConcurrentCreate):
		// 配置了 Locker 且并发：翻译成 "操作太频繁"
		return
	case err != nil:
		return
	}

	// result.Order 实现 OrderSnapshot；result.PaymentParams 供客户端拉起支付。
	// result.Reused == true 表示复用了已有 Pending 订单。
	fmt.Println(result.Order.OrderNo(), result.Reused)
}

// ExampleEngine_PollStatus 演示 APP 轮询订单状态。
// Engine 会做 userID 归属校验，不匹配返回 ErrOrderForbidden。
func ExampleEngine_PollStatus() {
	var engine *orderflow.Engine[*orderSnapshot]
	ctx := context.Background()

	res, err := engine.PollStatus(ctx, "order-token-abc", 1001)
	switch {
	case errors.Is(err, orderflow.ErrOrderNotFound):
		return // 404
	case errors.Is(err, orderflow.ErrOrderForbidden):
		return // 403，token 合法但用户不匹配
	case err != nil:
		return // 500
	}
	fmt.Println(res.StatusText) // pending / paid / delivered / ...
}

// ExampleEngine_Subscribe 演示 SSE 推送的首帧-订阅组合模式。
// 因为 Redis Pub/Sub 不保留历史，必须先 PollStatus 拿当前状态作首帧。
func ExampleEngine_Subscribe() {
	var engine *orderflow.Engine[*orderSnapshot]
	ctx := context.Background()

	// 1. 首帧——PollStatus 做鉴权 + 拿当前状态
	cur, err := engine.PollStatus(ctx, "order-token-abc", 1001)
	if err != nil {
		return
	}
	if cur.Status.IsTerminal() {
		return // 已是终态，无需订阅
	}

	// 2. 挂订阅
	sub, err := engine.Subscribe(ctx, "order-token-abc")
	if err != nil {
		return
	}
	defer func() { _ = sub.Close() }()

	// 3. 事件循环
	for status := range sub.Events() {
		fmt.Println(status)
		if status.IsTerminal() {
			return
		}
	}
}

// ExampleEngine_HandleNotify 演示支付回调 handler 的标准骨架。
// 配合 gateway.AckNotify 返回网关期望的响应体。
func ExampleEngine_HandleNotify() {
	var engine *orderflow.Engine[*orderSnapshot]
	var gateway orderflow.PaymentGateway // 来自 drivers/paymgrgw 等

	handler := func(w http.ResponseWriter, r *http.Request) {
		ch := orderflow.Channel(r.PathValue("channel"))

		// Engine 只在"致命错误"时返回 err（解析失败、CAS 系统错）；
		// 业务异常（金额不一致、已关闭等）已走 OnAnomaly，这里 err == nil。
		if err := engine.HandleNotify(r.Context(), ch, r); err != nil {
			http.Error(w, "internal", http.StatusInternalServerError)
			return
		}
		if err := gateway.AckNotify(ch, w); err != nil {
			engine.Logger().ErrorContext(r.Context(), "ack notify failed")
		}
	}

	_ = handler
}

// ExampleEngine_Close 演示后台 / worker 调用 Close 关闭超时订单。
// Close 对"非 Pending / 未过期 / 订单不存在"三种情况都幂等 skip。
func ExampleEngine_Close() {
	var engine *orderflow.Engine[*orderSnapshot]
	ctx := context.Background()

	if err := engine.Close(ctx, "ORD-2026042201"); err != nil {
		// 只在网关关闭失败或 DB CAS 系统错时返回 err；业务"不该关"的情况 Engine 内部 skip
		return
	}
}

// orderSnapshot 是供上面 Example 编译用的最小 OrderSnapshot 实现。
// 真实业务中应由你的 Order 模型实现（字段名、表结构自由）。
type orderSnapshot struct{}

func (*orderSnapshot) OrderNo() string               { return "" }
func (*orderSnapshot) OrderToken() string            { return "" }
func (*orderSnapshot) UserID() int64                 { return 0 }
func (*orderSnapshot) Status() orderflow.OrderStatus { return orderflow.StatusUnknown }
func (*orderSnapshot) ProductID() uint64             { return 0 }
func (*orderSnapshot) ProductType() string           { return "" }
func (*orderSnapshot) ProductTitle() string          { return "" }
func (*orderSnapshot) PayMethod() string             { return "" }
func (*orderSnapshot) PayAmount() int64              { return 0 }
func (*orderSnapshot) OriginalPrice() int64          { return 0 }
func (*orderSnapshot) TradeNo() string               { return "" }
func (*orderSnapshot) ExpireAt() time.Time           { return time.Time{} }
func (*orderSnapshot) PaidAt() (time.Time, bool)     { return time.Time{}, false }
func (*orderSnapshot) Extra() map[string]any         { return nil }
