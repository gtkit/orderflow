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
    ├── gormstore/             # module github.com/gtkit/orderflow/drivers/gormstore
    ├── rediscache/            # module github.com/gtkit/orderflow/drivers/rediscache
    └── rediszq/               # module github.com/gtkit/orderflow/drivers/rediszq
```

## driver 清单

| Driver | 实现接口 | 状态 | 依赖 |
|---|---|---|---|
| `paymgrgw` | `PaymentGateway` | **v0.4.0 已落地** | `github.com/gtkit/go-pay` |
| `gormstore` | `Store[O]` | **v0.4.1 已落地** | `gorm.io/gorm` |
| `rediscache` | `StatusCache` + `StatusStream` | **v0.4.2 已落地** | `github.com/redis/go-redis/v9` |
| `rediszq` | `DelayQueue` | **v0.4.3 已落地** | `github.com/redis/go-redis/v9` |

## 本地开发

当前仓库的 `go.work` 只把四个 driver 纳入 `use` 列表，并通过 workspace-level `replace github.com/gtkit/orderflow => .` 让 driver 在本地解析到当前核心包代码。

- 改核心包：在仓库根目录用 `GOWORK=off` 跑校验，例如 `GOWORK=off go test ./...`
- 改 driver：进入对应 driver 目录执行 `go test ./...`
- 正式发版时，driver `go.mod` 不允许保留本地 `replace`，以 `scripts/check-release.sh` 为准

## 发版与依赖维护

完整的依赖维护、版本号规则、发版流程和自检清单参见 [RELEASING.md](./RELEASING.md)。
