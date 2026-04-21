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
