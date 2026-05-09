# 发版指南（orderflow & drivers）

> 本仓库是多 module 单仓库（monorepo）：根 `github.com/gtkit/orderflow` 是核心包，`drivers/*` 各自是独立 Go module。
>
> 发布策略：**保留多 module，但统一一个 SemVer 版本号、多枚 Go module tag**。每次正式发布都同时创建根模块 tag 与所有 driver tag。

## 模块与 Tag

| 模块路径 | Tag 格式 |
|---|---|
| `github.com/gtkit/orderflow` | `vX.Y.Z` |
| `github.com/gtkit/orderflow/drivers/gormstore` | `drivers/gormstore/vX.Y.Z` |
| `github.com/gtkit/orderflow/drivers/paymgrgw` | `drivers/paymgrgw/vX.Y.Z` |
| `github.com/gtkit/orderflow/drivers/rediscache` | `drivers/rediscache/vX.Y.Z` |
| `github.com/gtkit/orderflow/drivers/rediszq` | `drivers/rediszq/vX.Y.Z` |

子模块 tag 必须带目录前缀，这是 Go Module 多 module 仓库的硬性规则。版本号部分必须一致，例如 `v1.10.0` 对应：

```text
v1.10.0
drivers/gormstore/v1.10.0
drivers/paymgrgw/v1.10.0
drivers/rediscache/v1.10.0
drivers/rediszq/v1.10.0
```

## 依赖维护

### 为 driver 新增或升级第三方依赖

必须在对应 driver 目录内操作：

```bash
cd drivers/paymgrgw
GOWORK=off go get github.com/gtkit/go-pay@v1.4.0
GOWORK=off go mod tidy
```

回到根目录后同步记录到 `CHANGELOG.md` 的 `[Unreleased]` 区段。若该依赖升级影响运行时行为，也要说明下游影响与兼容性。

### driver 对核心包版本

所有 driver 的 `go.mod` 应对齐当前统一版本，例如：

```go
require github.com/gtkit/orderflow v1.10.0
```

如果发版前需要先让 driver 获取新核心版本校验和，应先提交并推送 release commit，再创建根 tag，随后在 driver 中执行：

```bash
cd drivers/gormstore
GOWORK=off go mod download github.com/gtkit/orderflow@v1.10.0
GOWORK=off go mod tidy
```

## 版本号规则（SemVer 2.0.0）

全仓只选择一个版本号，取本次变更中最高影响级别：

| 变更性质 | 版本递增 |
|---|---|
| 不兼容 API / 行为变更 | MAJOR |
| 向后兼容的新能力、新 Option、依赖升级带来可见能力变化 | MINOR |
| Bug 修复、文档修正、内部重构、工具脚本改进 | PATCH |

`v0.x.y` 开发期仍按“破坏性变更至少升 MINOR”的原则提醒下游。`v2.0.0+` 必须同步调整 module path 的 `/v2`、`/v3` 后缀。

## 统一发版流程

### 1. 整理变更与 CHANGELOG

- 确认所有变更已提交或准备提交。
- 将 `CHANGELOG.md` 的 `[Unreleased]` 内容剪切到新版本区段，格式为 `## [X.Y.Z] - YYYY-MM-DD`。
- 破坏性变更必须在版本条目顶部用 **⚠ 破坏性变更** 标注。
- README / driver 文档涉及使用方式变化时必须同步更新。

### 2. 提交并推送 release commit

```bash
git status --short
git add CHANGELOG.md drivers/*/go.mod drivers/*/go.sum scripts drivers/RELEASING.md
git commit -m "chore(release): 准备 v1.10.0"
git push gtkit main
```

### 3. Dry-run 检查统一发版

```bash
scripts/release-all.sh v1.10.0
```

脚本会检查：

- 版本号格式是否为 `vX.Y.Z`
- 工作区是否干净
- 当前分支是否与远端同步
- `CHANGELOG.md` 是否已有对应版本区段
- 本地和远端是否已存在同名 tag

默认 dry-run 只打印将创建的 tag，不会修改本地或远端。

### 4. 创建并推送全部 tag

```bash
scripts/release-all.sh v1.10.0 --push
```

脚本会先创建并推送根 tag，然后自动将所有 driver 的 `require github.com/gtkit/orderflow` 对齐到本次版本；如 `go.mod` / `go.sum` 有变化，会自动提交 `chore(drivers): 对齐 orderflow v1.10.0` 并推送。随后脚本会先创建本地 driver tag，运行 `scripts/check-release.sh` 通过后，再推送所有 driver tag。

脚本会创建并推送附注 tag：

```text
v1.10.0
drivers/gormstore/v1.10.0
drivers/paymgrgw/v1.10.0
drivers/rediscache/v1.10.0
drivers/rediszq/v1.10.0
```

Tag message 使用简体中文，禁止轻量标签。

### 5. 发版后验证

```bash
git ls-remote --tags --refs gtkit | awk '{sub("refs/tags/", "", $2); print $2}' | sort -V
git status -sb
scripts/check-modules.sh
```

预期：远端 tag 集合包含本次统一版本；工作区干净；`check-modules.sh` 无需发版提示。

## 下游消费者视角

```go
require (
    github.com/gtkit/orderflow v1.10.0
    github.com/gtkit/orderflow/drivers/gormstore v1.10.0
)
```

下游只会拉入实际引用的 driver 传递依赖，不使用的 driver 依赖不会进入依赖图。

## 发版前自检清单

- [ ] `CHANGELOG.md` 已追加本版本区段，日期正确
- [ ] 使用方式变化已同步 README / driver 文档
- [ ] driver `go.mod` 无本地 `replace github.com/gtkit/orderflow`
- [ ] driver `require github.com/gtkit/orderflow` 指向本次统一版本或已发布的目标版本
- [ ] `GOWORK=off go vet ./...` 通过
- [ ] `bash scripts/lint-all.sh` 通过
- [ ] `GOWORK=off go test -race -count=1 -timeout=5m ./...` 通过
- [ ] `GOWORK=off go test -bench=. -benchmem -count=3 ./...` 通过
- [ ] `GOWORK=off go test -coverprofile=coverage.out ./...` 通过
- [ ] `scripts/check-release.sh` 通过
- [ ] `scripts/release-all.sh vX.Y.Z` dry-run 通过
- [ ] `scripts/release-all.sh vX.Y.Z --push` 已推送全部 tag

## 事故处理

### 发错版本号或漏发 tag

若 tag 已推送，原则上不要重命名、删除、强制覆盖；下游和 Go Proxy 可能已缓存。正确做法是打一个修复版本，并在 `CHANGELOG.md` 说明取代关系。

### driver tag 推了但核心 tag 忘推

driver `require` 的核心版本在 proxy 中不存在时，下游 `go get` 会失败。若核心代码 ready，立即推核心 tag；若不 ready，只能再打一个修复版本修正 driver 的 require。

### 敏感信息入库

按 `AGENTS.md` 的事故响应流程处理：立即吊销密钥，从 Git 历史清除，并记录到 `.harness/error-journal.md`。
