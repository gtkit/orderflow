# orderflow drivers

`drivers/` 目录存放 orderflow 能力接口（`Store` / `PaymentGateway` / `DelayQueue` / `StatusCache` / `StatusStream`）的默认实现。

## 模块边界设计

**每个 driver 是独立 Go module**（各自维护 `go.mod`），原因：

- 核心包 `github.com/gtkit/orderflow` 保持**零第三方依赖**；
- 用户只为实际导入的 driver 买单——不用 GORM 的项目不会被拖进 GORM 依赖图；
- driver 升级节奏可以独立于核心包。

目录结构：

```text
orderflow/
├── go.mod                     # module github.com/gtkit/orderflow (core, zero-dep)
└── drivers/
    ├── paymgrgw/              # module github.com/gtkit/orderflow/drivers/paymgrgw
    │   └── go.mod
    ├── gormstore/             # (规划中) module github.com/gtkit/orderflow/drivers/gormstore
    ├── rediscache/            # (规划中) module github.com/gtkit/orderflow/drivers/rediscache
    └── rediszq/               # (规划中) module github.com/gtkit/orderflow/drivers/rediszq
```

## driver 清单

| Driver | 实现接口 | 状态 | 依赖 |
|---|---|---|---|
| `paymgrgw` | `PaymentGateway` | **v0.4.0 已落地** | `github.com/gtkit/go-pay` |
| `gormstore` | `Store[O]` | **v0.4.1 已落地** | `gorm.io/gorm` |
| `rediscache` | `StatusCache` + `StatusStream` | **v0.4.2 已落地** | `github.com/redis/go-redis/v9` |
| `rediszq` | `DelayQueue` | **v0.4.3 已落地** | `github.com/redis/go-redis/v9` |

## 本地开发

sleep_client 的 `go.work` 同时 `use` 了核心包和 driver 子模块。各 driver 的 `go.mod` 通过 `replace github.com/gtkit/orderflow => ../..` 保证脱离 workspace 也能独立构建。正式 tag 时需同步把 `require` 里的核心包版本号升到对应版本。
