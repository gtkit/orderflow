# 退款编排的 Observer 事件命名约定

核心包**不参与退款编排**（见 `refund_gateway.go` 的设计说明）。但为了让跨项目监控
能用同一套 dashboard 看退款指标，核心包提供推荐的 `EventKind` / `AnomalyKind` 常量
和 attribute schema。`EventKind` 常量定义在 `observer.go`，`AnomalyKind` 常量定义在
`events.go`；业务方在自己的退款编排代码里调 `Observer.Event` 时使用这些常量即可。

## 事件列表与 emit 时机

| EventKind | 何时 emit | 必填 attrs | 可选 attrs |
|---|---|---|---|
| `EventRefundInitiated` | 业务方调 `RefundGateway.Refund` 之前 | `out_refund_no`, `out_trade_no`, `refund_amount`, `channel` | `reason`, `total_amount` |
| `EventRefundSucceeded` | 收到终态 `RefundTradeStatusSucceeded`（异步通知或 QueryRefund） | `out_refund_no`, `out_trade_no`, `refund_amount`, `channel`, `succeeded_at` | `transaction_id`, `gateway_refund_id`, `user_received_account` |
| `EventRefundFailed` | 收到终态 `RefundTradeStatusFailed` | `out_refund_no`, `out_trade_no`, `refund_amount`, `channel`, `reason` | `transaction_id`, `gateway_refund_id` |
| `EventRefundUnknown` | 收到非终态 `RefundTradeStatusUnknown`（**需人工介入**） | `out_refund_no`, `out_trade_no`, `channel`, `status` | `transaction_id`, `gateway_refund_id` |

## Anomaly 列表

| AnomalyKind | 触发场景 | 必填 attrs |
|---|---|---|
| `AnomalyRefundGatewayFailed` | `Refund` / `QueryRefund` 多次重试后仍返回非可忽略 error | `out_refund_no`, `channel`, `reason` |
| `AnomalyRefundDrift` | 异步通知声明的状态 ≠ `QueryRefund` 返回的状态（典型：通知 succeeded + query failed） | `out_refund_no`, `notify_status`, `query_status` |

## Attribute 类型约定

- `out_refund_no` / `out_trade_no` / `transaction_id` / `gateway_refund_id` / `channel` / `reason` / `status` / `notify_status` / `query_status` / `user_received_account`：**string**
- `refund_amount` / `total_amount`：**int64**（单位：分）
- `succeeded_at`：**time.Time**

## emit 模板

```go
// 退款发起
observer.Event(ctx, orderflow.EventRefundInitiated, order.OrderNo(), map[string]any{
    "out_refund_no": refundNo,
    "out_trade_no":  order.OrderNo(),
    "refund_amount": amount,
    "channel":       string(channel),
    "reason":        reason,
})

// 退款失败终态
observer.Event(ctx, orderflow.EventRefundFailed, order.OrderNo(), map[string]any{
    "out_refund_no": refundNo,
    "out_trade_no":  order.OrderNo(),
    "refund_amount": amount,
    "channel":       string(channel),
    "reason":        gatewayResp.Raw,
})

// 退款状态漂移异常
observer.Event(ctx, orderflow.EventAnomaly, order.OrderNo(), map[string]any{
    "kind":          string(orderflow.AnomalyRefundDrift),
    "out_refund_no": refundNo,
    "notify_status": notifyStatus.String(),
    "query_status":  queryStatus.String(),
})
```

## 设计原则

- **核心包不主动 emit 这些事件**——退款全部由业务方编排，emit 也由业务方负责。
- **核心包只定义命名**——常量值固定后业务方 dashboard 可跨项目复用。
- **schema 是推荐不是强制**——业务方可按需扩展 attrs，但**已列出的 key 必须保留语义**
  （例如 `refund_amount` 永远是分而不是元），否则 dashboard 解析逻辑会错乱。
- 完整 demo 见 `examples/refund_quickstart/main.go`。
