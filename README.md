# orderflow

`github.com/gtkit/orderflow` 提供可复用的订单流程引擎，封装了订单创建、支付超时关闭、支付回调处理、履约交付、状态推送和幂等补偿等一整套能力。业务方只需实现少量接口 + 业务钩子即可接入。

## 设计原则

- **核心包零第三方依赖**：只依赖 Go 标准库，gorm / go-redis / go-pay 等基础设施实现封装在后续的 `drivers/` 子包中。
- **泛型订单类型 `O`**：业务方的订单结构体通过实现 `OrderSnapshot` 接口接入，无需改造表结构。
- **能力接口 + 函数钩子的混合风格**：多方法的稳定能力用接口（`Store` / `PaymentGateway` 等），一次性业务决策用函数钩子（`OnPaid` / `OnClosed` 等）。

## 目录结构

```text
orderflow/
├── orderflow.go       Engine[O] + Config[O] + New
├── snapshot.go        OrderSnapshot 接口（业务 Order 必须实现）
├── status.go          OrderStatus + 合法跃迁表
├── spec.go            OrderSpec / ProductInfo / BillSpec
├── store.go           Store[O] 接口 + LogEntry
├── gateway.go         PaymentGateway + Channel + NotifyResult
├── delayqueue.go      DelayQueue 接口
├── cache.go           StatusCache + StatusStream
├── hooks.go           业务钩子函数类型
├── events.go          ClosedReason + AnomalyKind 枚举
├── errors.go          sentinel 错误
├── ordernum.go        默认 OrderNo / OrderToken 生成器
├── requests.go        CreateRequest / CreateResult / Timeline
└── doc.go             包文档
```

## 最小接入示例（当前仅支持构造，引擎核心方法随后续版本交付）

```go
import (
    "log/slog"

    "github.com/gtkit/logger"
    "github.com/gtkit/orderflow"
)

// 1) 让业务 Order 实现 OrderSnapshot
type MyOrder struct { /* ... */ }

func (o MyOrder) OrderNo() string          { /* ... */ }
func (o MyOrder) OrderToken() string       { /* ... */ }
// ... 其余 OrderSnapshot 方法

// 2) 桥接 gtkit/logger 到 slog
slog.SetDefault(slog.New(logger.SlogHandler()))

// 3) 构造 Engine
engine, err := orderflow.New[MyOrder](orderflow.Config[MyOrder]{
    Store:      myStore,      // 实现 orderflow.Store[MyOrder]
    Gateway:    myGateway,    // 实现 orderflow.PaymentGateway
    DelayQueue: myDelayQueue, // 实现 orderflow.DelayQueue
    Cache:      myCache,      // 实现 orderflow.StatusCache
    Stream:     myStream,     // 实现 orderflow.StatusStream

    OnPaid: func(ctx context.Context, o MyOrder, n orderflow.NotifyResult) error {
        // 典型：根据订单中的权益快照发放 VIP / 实物发货 / 积分入账
        return vipSvc.Activate(ctx, o.UserID(), o.Extra())
    },
    BuildNotifyURL: func(ch orderflow.Channel) string {
        return cfg.BaseURL + "/api/v1/orders/notify/" + string(ch)
    },
    Timezone: "Asia/Shanghai",
    Logger:   slog.Default(),
})
if err != nil {
    return err
}
```

## 版本路线

本仓目前处于 `v0.x` 开发期，不做兼容性承诺，任何破坏性变更至少递增 MINOR。

- `v0.1.0`：骨架落地，定义全部接口与类型
- `v0.2.0`：实现 `Engine.Create / PollStatus / Timeline / ListUserOrders / Subscribe / HandleNotify / Close / ReconcilePaid` 核心方法
- `v0.3.0`：`worker/` 子包（`CloseWorker` / `CloseFallback` / `DeliveryFallback` + `StartAll` 助手）
- `v0.4.0`：`drivers/paymgrgw` 首个 driver 落地；独立 go module，核心包保持零依赖
- `v0.4.1`：`drivers/gormstore` 落地——双泛型 `Store[O, M]`，ColumnMap + 内置 Bill/Log 模型 + FinalizeExtra 事务钩子
- `v0.4.2`：`drivers/rediscache` 落地——`StatusCache` 紧凑字符串编码 + `StatusStream` Pub/Sub + 按状态派 TTL
- `v0.4.3`：`drivers/rediszq` 落地——Redis ZSET 延迟队列 + 双 ZSET 租约模型，12 个集成测试 `-race` 通过（**当前版本**）
- `v1.0.0`：稳定化，进入 SemVer 严格模式

**至此 4 个 driver 全部落地**，orderflow 可以在任何带 MySQL + Redis + 微信/支付宝支付的项目中开箱接入。

## Worker 接入示例

```go
import "github.com/gtkit/orderflow/worker"

engine, _ := orderflow.New[MyOrder](cfg)

// 默认节奏（PollInterval=1s / BatchSize=50 / MaxWorkers=15 / ...）
go worker.StartAll(ctx, engine)

// 自定义节奏
go worker.StartAllWithOptions(ctx, engine, worker.Options{
    Close:            worker.CloseOptions{PollInterval: 2 * time.Second, MaxWorkers: 32},
    CloseFallback:    worker.CloseFallbackOptions{Interval: 10 * time.Minute},
    DeliveryFallback: worker.DeliveryFallbackOptions{Interval: 30 * time.Second},
})
```

## 查询与订阅：轮询 + SSE

订单状态读路径有两条，**面向不同客户端场景，实践上通常同时使用**：

| 能力 | API | 底层接口 | 适用 |
|---|---|---|---|
| 轮询 | `Engine.PollStatus` | `StatusCache` | APP 定时拉，网络差、兼容性最好 |
| 推送 | `Engine.Subscribe` | `StatusStream` | SSE / WebSocket 实时通知 |
| 流水 | `Engine.Timeline` | `Store.ListLogsByOrderNo` | 订单详情页展示变更历史 |

**重要约束**：

- `Engine.Subscribe` 不做 `userID` 归属校验（直接透传到 driver），**业务侧 handler 必须自己鉴权**；`PollStatus` / `Timeline` 会做（传错 userID 返回 `ErrOrderForbidden`）。
- Redis Pub/Sub **不保留历史**——Subscribe 只能收到订阅建立之后的事件。所以 SSE handler 必须"先 Poll 拿当前状态作首帧，再挂 Subscribe 接后续变更"，否则会丢失订阅建立瞬间的跃迁。
- `Subscription.Close` 幂等，`defer sub.Close()` 无副作用。
- 进入终态（`OrderStatus.IsTerminal` 为 true）后不会再有事件，handler 可直接结束连接。

### 轮询 handler

```go
func pollHandler(w http.ResponseWriter, r *http.Request) {
    userID := authFromCtx(r.Context())  // 从鉴权上下文取，禁止读 query/body
    orderToken := r.PathValue("token")

    res, err := engine.PollStatus(r.Context(), orderToken, userID)
    switch {
    case errors.Is(err, orderflow.ErrOrderNotFound):
        http.Error(w, "not found", http.StatusNotFound)
        return
    case errors.Is(err, orderflow.ErrOrderForbidden):
        http.Error(w, "forbidden", http.StatusForbidden)
        return
    case err != nil:
        http.Error(w, "internal", http.StatusInternalServerError)
        return
    }

    _ = json.NewEncoder(w).Encode(res)  // {"Status":2,"StatusText":"paid"}
}
```

### SSE handler（Poll 首帧 + Subscribe 后续）

```go
func sseHandler(w http.ResponseWriter, r *http.Request) {
    userID := authFromCtx(r.Context())
    orderToken := r.PathValue("token")

    // 1. 先用 PollStatus 做归属校验 + 拿当前状态作首帧。
    //    Subscribe 建立前的跃迁只能从这里拿到，漏掉就追不回来了。
    cur, err := engine.PollStatus(r.Context(), orderToken, userID)
    switch {
    case errors.Is(err, orderflow.ErrOrderNotFound):
        http.Error(w, "not found", http.StatusNotFound)
        return
    case errors.Is(err, orderflow.ErrOrderForbidden):
        http.Error(w, "forbidden", http.StatusForbidden)
        return
    case err != nil:
        http.Error(w, "internal", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "streaming unsupported", http.StatusInternalServerError)
        return
    }

    writeEvent := func(s orderflow.OrderStatus) {
        fmt.Fprintf(w, "event: status\ndata: {\"status\":%d,\"text\":%q}\n\n", s, s.String())
        flusher.Flush()
    }

    // 2. 首帧 —— 当前状态。终态直接结束，无需订阅。
    writeEvent(cur.Status)
    if cur.Status.IsTerminal() {
        return
    }

    // 3. 挂订阅接后续变更。
    sub, err := engine.Subscribe(r.Context(), orderToken)
    if err != nil {
        return  // 首帧已发，客户端据此兜底轮询即可
    }
    defer sub.Close()

    // 4. 事件循环：客户端断开 / driver 关闭 channel / 进入终态都会退出。
    keepalive := time.NewTicker(15 * time.Second)
    defer keepalive.Stop()

    for {
        select {
        case <-r.Context().Done():
            return
        case status, ok := <-sub.Events():
            if !ok {
                return  // driver 主动关闭
            }
            writeEvent(status)
            if status.IsTerminal() {
                return
            }
        case <-keepalive.C:
            fmt.Fprint(w, ": ping\n\n")  // 注释行，防止中间代理切连接
            flusher.Flush()
        }
    }
}
```

**客户端侧建议**：SSE 断线后降级为 `PollStatus` 轮询到终态，不要无限重连 SSE——既省连接数，又能避开代理/网关 30s 空闲超时造成的断流。

## 钩子执行时序（重要）

`OnPaid` 是"支付确认后、订单写 Delivered 前"被调用的业务钩子。
Engine 的履约流程为：

```text
CAS Pending -> Paid   (Store.CASConfirmPaid)
       │
       ├── OnPaid(ctx, order, notify)        业务侧权益发放（VIP / 发货 / 积分）
       │                                     必须幂等 —— fallback scanner 会重试
       │
       ├── Store.FinalizePaidOrder(order, bill)   订单 -> Delivered，写账单
       │
       ├── StatusCache.Set(Delivered) + StatusStream.Publish(Delivered)
       │
       └── OnDelivered(ctx, order)           旁路观察（失败仅告警，不影响主流程）
```

**关键不变量**：
- 任何一步失败，Engine 都不会对支付网关返回错误（避免重试风暴）；
- 失败订单会停留在 `Paid` 状态，由 `ReconcilePaid` 供 fallback worker 后续补偿；
- 因此 `OnPaid` 必须幂等：重复调用不产生副作用。

## 相关约束

- Go 1.26.2+
- 日志通过 `*slog.Logger` 注入；接入 `github.com/gtkit/logger` 使用 `slog.New(logger.SlogHandler())`。
- 后续引入 JSON 序列化时，driver 侧统一使用 `github.com/gtkit/json`。
