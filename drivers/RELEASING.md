# 发版指南（orderflow & drivers）

> 本仓库是多 module 单仓库（monorepo）：根 `github.com/gtkit/orderflow` 是零依赖核心包，
> `drivers/*` 各自是独立 Go module，各自独立 tag、独立版本。
>
> 本指南覆盖**依赖维护**与**发版流程**全部操作。发版前请对照最后的清单逐项勾选。

## 模块拓扑

```
orderflow/                              module: github.com/gtkit/orderflow              tag: vX.Y.Z
└── drivers/
    ├── paymgrgw/                       module: .../drivers/paymgrgw                    tag: drivers/paymgrgw/vX.Y.Z
    ├── gormstore/                      module: .../drivers/gormstore                   tag: drivers/gormstore/vX.Y.Z
    ├── rediscache/                     module: .../drivers/rediscache                  tag: drivers/rediscache/vX.Y.Z
    └── rediszq/                        module: .../drivers/rediszq                     tag: drivers/rediszq/vX.Y.Z
```

关键约束：

- 根 `go.mod` **零第三方依赖**，不允许 `go get` 任何外部包进根模块。
- driver `go.mod` **禁止出现** `replace github.com/gtkit/orderflow => ...`，由 `scripts/check-release.sh` 守门。
- `go.work` **仅本地 / CI 生效**，发版后下游消费者不会读到它。
- 所有 tag 必须是附注标签（`git tag -a`），消息使用简体中文。

## 本地开发

根目录 `go.work` 已把四个 driver 纳入 workspace，并通过 `replace github.com/gtkit/orderflow => .`
让 driver 的 `require github.com/gtkit/orderflow vX.Y.Z` 解析到当前目录代码。

常规流程：

```bash
# 改核心代码
go test ./...

# 改 driver
cd drivers/gormstore && go test ./...
```

workspace 模式下不需要修改任何 driver 的 `go.mod`。

## 依赖维护

### 为 driver 新增或升级第三方依赖

**必须在 driver 目录内操作**：

```bash
cd drivers/gormstore
go get gorm.io/gorm@v1.31.1
go mod tidy
```

回到根目录同步 workspace 锁：

```bash
go work sync
```

### 核心包依赖

核心包 `github.com/gtkit/orderflow` 不引入任何第三方依赖。如果确实需要（例如 JSON
场景），按 `AGENTS.md` 只允许 `github.com/gtkit/*`，并且必须在 CHANGELOG 标注。

## 版本号规则（SemVer 2.0.0）

| 变更性质 | 核心 | driver |
|---|---|---|
| 修改核心导出 API 签名 / 行为 | MAJOR | 适配后至少 MINOR，破坏性适配 MAJOR |
| 核心新增接口方法（driver 必须实现） | MAJOR | 所有相关 driver MAJOR |
| 核心新增可选 Option / 新导出函数 | MINOR | driver 可不动 |
| driver 内部 bugfix | — | PATCH |
| driver 升级第三方主版本（如 gorm v1→v2） | — | MAJOR |
| 文档修正、内部重构（不改行为） | PATCH | PATCH |

### v2+ 额外要求

升到 `v2.0.0` 及以上的模块，`go.mod` 的 module path 必须带 `/v2`、`/v3` 后缀：

- 核心包：`module github.com/gtkit/orderflow/v2`
- driver：`module github.com/gtkit/orderflow/drivers/gormstore/v2`

目录需通过子目录（`v2/`）或主干分支提供该 major 版本，这是 Go Module 硬性规定。

## 核心包发版流程

### 1. 落定代码与 CHANGELOG

- 确认所有改动已合并到 `main`。
- 将 `CHANGELOG.md` 的 `[Unreleased]` 区段剪切到新版本区段，附日期 `YYYY-MM-DD`。
- 破坏性变更在条目顶部用 **⚠ 破坏性变更** 标注。

### 2. 根目录跑全量检查

```bash
go vet ./...
golangci-lint run ./...
go test -race -count=1 -timeout=5m ./...
go test -bench=. -benchmem -count=3 ./...
go test -coverprofile=coverage.out ./...
```

### 3. 打核心 tag 并推送

```bash
git tag -a v1.1.0 -m "版本 v1.1.0

主要变更：
- feat: 新增 xxx
- fix: 修复 xxx

破坏性变更（如有）：
- BREAKING CHANGE: xxx 已删除，请使用 yyy 替代
"
git push origin v1.1.0
```

**核心 tag 必须先于 driver tag 推送**——driver 要 require 真实存在的核心版本。

## driver 发版流程

以 `drivers/gormstore` 为例，其它 driver 同理。

### 1. 升级 require 的核心版本

```bash
cd drivers/gormstore
go get github.com/gtkit/orderflow@v1.1.0
go mod tidy
```

### 2. 本地测试（workspace 生效）

```bash
go test -race -count=1 ./...
```

### 3. 脱离 workspace 再测一次

**这是关键步骤**——模拟下游消费者的真实解析路径：

```bash
GOWORK=off go test -race -count=1 ./...
```

如果此步失败，通常意味着：

- 漏升核心 tag 版本
- 残留了本地 replace
- 依赖了 workspace 里的其它未发布 driver

### 4. 发版守门

回到仓库根：

```bash
cd ../..
scripts/check-release.sh
```

必须输出 `OK: all drivers' go.mod are release-ready`。

### 5. 更新 driver CHANGELOG（若维护了独立 CHANGELOG）

根 `CHANGELOG.md` 用 driver 范围条目统一记录即可，例如：

```
### Added
- `drivers/gormstore`: 新增 `WithAutoMigrate` Option（#123）
```

### 6. 打 driver tag

tag 名必须带目录前缀，这是 Go Module 多 module 仓库的硬性规定：

```bash
git tag -a drivers/gormstore/v1.1.0 -m "版本 drivers/gormstore/v1.1.0

主要变更：
- feat: 新增 xxx
- fix: 修复 xxx
"
git push origin drivers/gormstore/v1.1.0
```

四个 driver 互不绑定，按需逐个发版。

## 下游消费者视角

```go
// go.mod
require (
    github.com/gtkit/orderflow v1.1.0
    github.com/gtkit/orderflow/drivers/gormstore v1.1.0
)
```

消费者只会拉入实际引用的 driver 的传递依赖，不使用的 driver（以及它们的 GORM / Redis /
go-pay 等依赖）完全不会出现在依赖图中。

## 发版前自检清单

核心包发版：

- [ ] 根 `go.mod` 仍然零第三方依赖
- [ ] `go vet` / `golangci-lint` / `go test -race` 全部通过
- [ ] `CHANGELOG.md` 已追加本版本条目，日期正确
- [ ] 破坏性变更已在 CHANGELOG 与 tag message 中明确标注
- [ ] 所有导出 API 的 GoDoc 与 Example 测试齐全
- [ ] tag 是附注标签（`git tag -a`），message 使用简体中文
- [ ] tag 已推送到远端

driver 发版：

- [ ] driver `go.mod` 无 `replace github.com/gtkit/orderflow`
- [ ] driver `require github.com/gtkit/orderflow` 指向**已推送**的正式 tag，不是 `v0.0.0`
- [ ] `scripts/check-release.sh` 输出 OK
- [ ] 已在 `GOWORK=off` 下跑过 `go test -race`
- [ ] `CHANGELOG.md` 已追加 driver 维度的条目
- [ ] driver tag 名带 `drivers/<name>/` 前缀
- [ ] driver tag 是附注标签，message 使用简体中文
- [ ] driver tag 已推送到远端

## 事故处理

### 发错版本号

**禁止重命名、删除、强制覆盖已推送的 tag**，下游可能已缓存。正确做法：

- 打一个修复版本（例如 `v1.1.1` 或 `drivers/gormstore/v1.1.1`）。
- 在 CHANGELOG 中说明该版本取代了错误版本。

### driver tag 推了但核心 tag 忘推

driver `require` 的核心版本此时在 proxy 中不存在，下游 `go get` 会失败。立刻推核心 tag；
如果核心代码本身还没 ready，只能再打一个 driver 修复 tag 降级 require。

### 敏感信息入库

按 `AGENTS.md` 的事故响应流程：立即吊销密钥，从 Git 历史清除，记录到 `.harness/error-journal.md`。
不要试图靠 tag / commit 删除来"掩盖"——推送过的内容视作已泄露。
