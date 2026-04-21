# Changelog

本项目遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)，并使用 [语义化版本 2.0.0](https://semver.org/lang/zh-CN/)。

## [Unreleased]

_尚无未发布变更。_

## [1.0.0] - 2026-04-21

首个稳定版本。此前所有 v0.x 开发增量（骨架、5 核心方法、worker 子包、4 个 driver、
全套测试、review 修复、混沌测试、Observer / Locker / 幂等 helper、错误路径补齐）
全部进入本版本。从此遵守严格 SemVer。

### 里程碑概要

**核心 API**（`github.com/gtkit/orderflow`）：
- `Engine[O]` 8 个对外方法：`Create` / `PollStatus` / `Timeline` / `ListUserOrders` /
  `Subscribe` / `HandleNotify` / `Close` / `ReconcilePaid` + `CloseByUser` + `FindExpiredPending` /
  `FindPaidUndelivered` passthrough。
- 5 个能力接口：`Store[O]` / `PaymentGateway` / `DelayQueue` / `StatusCache` / `StatusStream`。
- 7 类业务钩子：`OnCreated` / `OnPaid` / `OnDelivered` / `OnClosed` / `OnReopened` /
  `OnSuperseded` / `OnAnomaly`。
- `Locker` 可选分布式锁 + `IdempotentOnPaid` 幂等 helper。
- `Observer` 观测接口（Event + Duration）。
- 7 个 sentinel 错误，按 `errors.Is` 识别。

**Worker**（`github.com/gtkit/orderflow/worker`）：
- `CloseWorker` 消费延时队列（退避 / 优雅关停 / Stats 快照）。
- `CloseFallback` / `DeliveryFallback` 周期扫描兜底。
- `StartAll` 一键启动全部 worker。

**Drivers**（各自独立 go.mod）：
- `drivers/paymgrgw`：基于 `github.com/gtkit/go-pay` 的 `PaymentGateway` 实现。
- `drivers/gormstore`：基于 GORM 的 `Store[O]`，双泛型 `[O, M]` + `ColumnMap` + 事务钩子。
- `drivers/rediscache`：`StatusCache` / `StatusStream` / `Locker` / `IdempotentOnPaidViaRedis`。
- `drivers/rediszq`：基于 Redis ZSET + Lua 的 `DelayQueue`。

**质量保障**：
- 199 个测试函数，`go test -race -count=10` 稳定通过。
- 核心包覆盖率 91.7%，driver 平均 > 75%（业务逻辑 > 95%）。
- 端到端闭环测试 3 个 narrative 场景（happy path / 超时关闭 / Closed→Paid 恢复）。
- 混沌测试 5 个场景（并发、竞态、时钟回拨、fallback 收敛）。
- 安全专项审查 + 并发专项审查 + code review 共 34 条问题落地修复。

**生产就绪检查清单**：
- 并发安全、SQL 注入防御、OrderToken 真随机、ParseNotify 强合约、输入长度校验、
  PII 日志脱敏建议、运维指标告警关键字、Redis 集群 hash tag 提示、发版 CI 校验脚本。

### Added (v0.7.2 错误路径 + 状态机死角覆盖)

生产上线前的最后一轮全流程测试补强——专门针对"I/O 失败的错误分支"和"CAS race 的非常见分支"，两类路径都是线上事故的典型入口。

**错误路径专项**（新 `error_paths_test.go`，15 个测试）：

每条 Engine 方法里 `if err != nil { ... }` 的分支都有专项断言：
- `Create`：Store.Create 错误 / Gateway.UnifiedOrder 错误 / rollbackPendingOnEnqueueFail 的 CAS error 和 affected=0 双分支 / Cache.Set 失败降级 / AppendLog 失败不阻断
- `PollStatus` / `Timeline`：Store.GetByToken 错误透传
- `HandleNotify`：ParseNotify 错误 / GetByNo 错误 / CASConfirmPaid 错误
- `Close`：GetByNo 错误 / CAS 错误
- `CloseByUser`：GetByNo 错误（不应被误分类为 NotFound/Forbidden）
- `ReconcilePaid`：GetByNo 错误
- `publishStatus` 双失败降级：cache.Set 失败 + cache.Delete 被调用的一致性兜底路径

**状态机死角专项**（新 `engine_subscribe_test.go` + 补齐 `engine_notify_test.go`，5 个测试）：

`recheckAfterCASFailed` 原先覆盖率只有 46%，补齐 4 个分支：
- CAS race 后订单变成 `Closed` → 走 `handleClosedPaidNotify` 恢复路径
- CAS race 后订单变成 `Delivered` → 静默日志跳过
- CAS race 后订单变成 `Cancelled`（未预期状态）→ Anomaly UnexpectedStatus
- CAS race 后订单消失 → ALERT 日志

为实现上述测试，fakeStore 新增两个精细竞态注入：
- `ConfirmPaidRaceToStatus OrderStatus`：让 CAS 返回 0 并把状态改为指定值（任意目标状态）
- `ConfirmPaidMakeDisappearOnce bool`：让 CAS 返回 0 并从 store 删除订单

此外 `Subscribe` 从 0% 覆盖补到 100%（透传语义确认）。

**稳定性验证**：全模块 `go test -race -count=10` 通过，无 flaky。

**核心包覆盖率从 83% 提升到 91.7%**。剩余未覆盖的主要是：
- `nopObserver.Event/Duration`（0%）：no-op 默认实现，测试场景都注入了 recording observer，不需要覆盖
- `resolveChannelOf / buildNotifyURLOf / isReusableOf` 的默认分支（~66%）：hook 未注入时走默认逻辑，已被 `standardRequest` 隐式覆盖
- `reloadOrder` 失败分支（50%）：仅在"初次 GetByNo 成功 + 重载 GetByNo 失败"的极端窗口触发，代码路径只 return 原 order（stale 但仍正确），非关键

### Added (v0.7.0 / v0.7.1 并发 Create 串行化 + OnPaid 幂等保护)

针对 v0.5.3 混沌测试发现的两个设计限制提供第一方解决方案：

**v0.7.0 核心（新 `locker.go`）**：

- 新增 `Locker` 接口：`TryLock(ctx, key, ttl) (unlock, ok, err)`——非阻塞分布式锁抽象。默认 `Config.Locker=nil` 行为不变（向后兼容）。
- 新增 `Config.Locker` + `Config.CreateLockTTL`：Engine.Create 在注入 Locker 后对 `(user_id, product_id)` 维度串行化，并发冲突返回 `ErrConcurrentCreate`。
- 新增 `ErrConcurrentCreate` sentinel：业务层翻译为"操作太频繁，请稍后再试"。
- 新增 `IdempotentOnPaid(inner, markerExists)` helper：用外部 marker 实现 OnPaid 至多一次成功的幂等包装。marker 查询由业务方提供（可基于自有幂等表、订单状态字段、Redis 键等）。

**v0.7.1 Redis driver（`drivers/rediscache/locker.go`）**：

- 新增 `rediscache.Locker`：基于 Redis `SET NX EX` + Lua `CAS 释放` 的生产级分布式锁。
  - 唯一 16 字节 token 作为持有凭据（防 A 的 unlock 误删 B 的锁）；
  - Lua CAS `if GET==token then DEL`；
  - `WithLockerKeyPrefix` 支持多项目共享 Redis；
  - unlock 用独立 `context.Background()` + 3s 超时，避免父 ctx 取消导致锁残留。
- 新增 `rediscache.IdempotentOnPaidViaRedis` helper：完整的 OnPaid 幂等保护，**无需业务侧 marker 表**。
  - `SET NX EX` 作为幂等凭据；
  - inner 成功 → 凭据保留（TTL 期后自动清理）；
  - inner 失败 → 立即 DEL 凭据允许 fallback 重试；
  - 并发 20 goroutine 调同订单，保证 inner 仅被调 1 次（`TestIdempotentOnPaidViaRedis_ConcurrentSingleInnerCall`）。

**设计决策说明**：
- 原方案中的 `IdempotentOnPaidViaBill` 被重新设计为更通用的 `IdempotentOnPaid`——原因是 Engine 当前时序（OnPaid 先 Finalize 后）下，用"bill 是否存在"做 marker 在 "OnPaid 成功但 Finalize 失败" 的窗口期会导致 retry 重复调 OnPaid。新方案让业务方自选 marker 源，配合 Redis 版 helper 提供真正安全的默认选项。
- Locker 默认值为 `nil`：**未注入则行为与 v0.6 完全一致**，现有业务升级无感知；需要串行化保证时才注入。
- 推荐三层防御：前端 debounce + Engine Locker + DB 部分唯一索引。

**测试覆盖**（新增 ~20 个测试）：
- core: Locker 4 场景（happy / 冲突 / 错误 / 并发 50 goroutine 串行化验证）+ IdempotentOnPaid 4 场景（跳过 / 调用 / marker 错误 / inner 错误）；
- rediscache: Locker 6 场景（含 TTL 自动过期、Lua CAS 防误删、前缀自定义、并发串行化）+ IdempotentOnPaidViaRedis 4 场景（首次调用 / 幂等跳过 / 失败重试 / 20 并发 exactly-once）。

**非破坏性变更**：所有新 API 都是 opt-in，现有业务代码零改动编译通过。

### Added (v0.6.0 Observer / Worker Stats / CloseByUser / E2E 闭环)

生产运维核心能力 + 端到端闭环验证。

**Observer 接口**（`observer.go` 新文件）：

- `Observer` interface 两个方法：
  - `Event(ctx, kind EventKind, orderNo, attrs)`：状态跃迁事件（Created / Reused / Superseded / Paid / Delivered / Closed / Reopened / Anomaly）；
  - `Duration(ctx, op, d, err)`：操作耗时（Create / HandleNotify / Close / ReconcilePaid）。
- 默认 `nopObserver`（零开销），业务侧注入 Prometheus / OpenTelemetry / slog 适配器。
- Engine 在 Create / HandleNotify / Close / ReconcilePaid 的方法入口 / 出口用 defer 打点记录 duration；在关键状态跃迁点发 Event。`recordAnomaly` 也同步 emit Anomaly event。
- 实现契约（doc 已明示）：Observer 实现**必须非阻塞且禁止 panic**——Engine 不加 recover，panic 会沿调用链传播。

**Worker Stats**（`worker/stats.go` 新文件）：

- `CloseWorker.Stats()` 返回线程安全快照：`Inflight` / `LastPollAt` / `LastPollDuration` / `LastBatchSize` / `LastError` / `PollsTotal` / `PollErrors`。
- 全部字段用 `atomic` 原语读写，调用方可从 Prometheus Collector 等并发 goroutine 安全读取。
- 接入指引：`Inflight` 做 gauge；`LastPollAt` 差值做 "worker alive" 探活；`PollErrors/PollsTotal` 比值做错误率告警。

**CloseByUser 便利 API**（`engine_close.go`）：

- `Engine.CloseByUser(ctx, userID, orderNo) error`：先 `GetByNo` + `UserID()` 归属校验（不匹配返 `ErrOrderForbidden`），再走标准 Close 流程。适用于"我的订单 → 取消"接口，避免业务方手写 UserID 校验漏掉。
- 未过期 Pending 订单仍被 Close skip（Close 对此幂等跳过），如需"立即取消未过期订单"应业务层自己做或扩展 API。

**E2E 闭环测试**（`e2e_test.go` 新文件）：

三个 narrative 测试把 orderflow 全部能力串起来验证每一步所有信号闭环：

- `TestE2E_FullOrderLifecycle`：Create → 入队 → 缓存 Pending → 客户端 Poll → HandleNotify（Paid）→ Finalize → 缓存 Delivered → Poll 再查 → Timeline → 重入 HandleNotify 幂等跳过。每一阶段断言 Store/Cache/Stream/DelayQueue/Gateway/Hook/Observer 所有维度。
- `TestE2E_TimeoutCloseCycle`：Create → 快进过期 → Close → 客户端 Poll 看到 Closed，Observer 记录 `order_closed` + reason=timeout。
- `TestE2E_CloseThenPaidRecovery`：已 Closed 订单收到延迟到达的支付回调 → 网关 Query 确认已支付 → CASReopenPaid → Delivered，OnReopened + OnPaid + OnDelivered 钩子与对应 observer event 全触发。

**专项回归测试**：

- `observer_test.go`（9 个测试）：Observer 事件/duration 在 Create 成功/复用/失败、HandleNotify 全流程、Close、Anomaly 场景下正确触发。
- `worker/stats_test.go`（5 个测试）：初始状态、空 poll、有任务 poll、错误注入路径。
- CloseByUser 的三个场景（成功 / 跨用户 Forbidden / 订单不存在）。

**非破坏性变更**：Config 新增 `Observer` 字段（零值为 nop），现有业务代码无需修改即可编译通过。

### Added (v0.5.3 混沌测试套件)

新增 `chaos_test.go` 覆盖 5 个生产故障场景的**端到端最终一致性**验证（`-race -count=3` 通过）：

| 场景 | 验证内容 |
|---|---|
| 并发 HandleNotify 不同订单 | 100 并发通知全收敛到 Delivered，bills/OnPaid/OnDelivered 各 100 次，零 Anomaly |
| HandleNotify vs Close 同单竞态 | 50 轮随机调度，终态必为 Delivered 或 Closed（绝不卡 Pending）；日志证实两种结局都出现 |
| OnPaid 瞬时失败 → DeliveryFallback 收敛 | 第 1 次 OnPaid 失败 → 订单停 Paid → ReconcilePaid 捞回 → 最终 Delivered；OnPaid 调用 2 次证实 fallback 工作 |
| 时钟回拨下 OrderNo 单调唯一 | 强制初始状态到"未来"1s，10k 并发生成不出现重复，且 ms 不倒退 |
| 并发同用户同商品 Create | 50 并发 Create 全成功无 Anomaly；**发现并文档化 Engine 已知限制**：跨 Create 不串行化 |

**混沌测试发现的真实问题**（并已修复）：

- **fakeStore 内部指针泄漏**（测试基础设施 bug）：GetByNo/GetByToken/ListByUser/FindPending/Create 返回内部可变 `*testOrder`，Engine 持有后被 CAS 方法原地修改产生 race。修复：所有 fake 读方法返回字段深拷贝（仿照真 DB 的 SELECT 返回独立行语义）。
- **fakeGateway 无锁**（测试基础设施 bug）：并发调用 UnifiedOrder 等方法时调用计数器 race。加 mutex。
- **Engine 不做跨 Create 串行化**（**生产代码真实限制**）：`FindPendingByUserAndProduct` 仅 SELECT 不锁行，并发下多个 goroutine 会同时读到"无 Pending"各自 INSERT，可能产生多个 Pending 订单。不是 bug 但必须文档化。业务层若需严格"一用户一商品一 Pending"必须自行幂等（分布式锁 / DB 唯一索引 / 前端 debounce）。这个发现仅靠单元测试看不到，是混沌测试的直接价值。

### Changed / Fixed (v0.5.2 安全 + 并发专项审查修复)

在 v0.5.1 通用 review 之后追加两轮并行专项审查（安全 / 并发），共发现 21 条新问题。本次落地其中 14 条高影响项，5 条文档强化，2 条待业务方决策。

**⚠ 破坏性变更（SECURITY FIX）**：

- **默认 OrderToken 生成器从 SHA-256 确定性哈希改为 16 字节 crypto/rand**——旧实现 `SHA-256(orderNo | userID | productID)[:16]` 对任何拿到三元组的人（对账文件、客服工单、日志归档）都可离线重算 token，等于没防护。新实现 128 bit 熵真正不可预测。业务若需确定性 token，自行注入带服务端 secret 的 HMAC 实现（hooks.go 文档已指引）。该变更在 v0.x 允许。

**Critical 修复**：

- **【并发 C1】默认 OrderNo 生成器改用原子 snowflake 状态机**：旧实现分两步取 `time.Now()` 和 `atomic.Add(seq)`，并发下可能交错产生字典序 ≠ 生成序的订单号；新实现用 atomic.Uint64 CAS 合并 (ms, seq)，字典序 = 单进程内生成顺序。附带 `TestAdvanceOrderNoState_StrictMonotonic` 和 `TestDefaultGenerateOrderNo_LexicographicOrderUnderRace` 两个并发回归测试。
- **【安全 C1】OrderToken 真正不可预测**：见上"破坏性变更"。附带 `TestDefaultGenerateOrderToken_Unpredictable` 回归测试。
- **【安全 C2】`gormstore` 表名（OrderTable / BillTable / LogTable）也走 SQL 标识符白名单校验**——与 ColumnMap 列名同等保护级别，防止外部配置（yaml / consul / env）通过表名注入 SQL。

**Important 修复**：

- **【并发 I1】CloseWorker processOne 的 closeCtx 改用 `context.WithoutCancel`**：父 ctx 取消（SIGTERM）不再立即打断在途的 gateway CloseOrder，让它们跑满自己的 CloseTimeout 预算。避免 graceful shutdown 期间所有在途任务被 ctx.Canceled 导致下次 Requeue 全部重跑（放大下游 gateway 压力）。
- **【并发 I2】rediscache Subscription forward 内层 select 加非阻塞前置检查**：Go select 随机性会让 `events <-` 在 done / ctx 同时 ready 时以 1/3 概率被选中，导致 Close 后仍发出一条事件。两段 select（先 non-blocking 查关闭信号再进阻塞写）让 done / ctx 严格优先。
- **【并发 I4】`rollbackPendingOnEnqueueFail` 的 CAS 失败路径显式记日志**：避免 "Redis 入队失败 + CAS 也失败"的孤儿 Pending 订单在 CloseFallback 兜底前完全无可观测性。
- **【安全 I2】`HandleNotify` 字段长度校验**：`OutTradeNo` ≤ 64、`TransactionID` ≤ 128，防止 DB 列截断引发的比较失真 + `GetByNo` 超长 key 触发的索引扫描 DoS。
- **【安全 I3】`Create` 入参长度校验 + ClientIP 合法性**：`Product.Title` 1–128、`Product.Type` ≤ 32、`PayMethod` 1–32、`ClientIP` 通过 `net.ParseIP`（拒绝 CRLF 注入等伪造）。
- **【安全 I4】`FindExpiredPending` / `FindPaidUndelivered` 的 limit 上限 clamp**：`MaxFindLimit = 1000`，防极大 limit 触发百万行扫描 OOM。
- **【安全 I7】`buildNotifyURLOf` 默认实现用 `url.PathEscape`**：保护未经业务方校验的 PayMethod 不通过 resolveChannelOf 产生 `/../admin` 路径遍历。

**文档契约强化**：

- `gateway.go` · `PaymentGateway` 接口：**ParseNotify 必须完成签名验证**改为强合约（不是"建议"）。
- `hooks.go` · `OnPaidHook`：补"同步执行约束"——禁止钩子内异步 goroutine + return nil。
- `hooks.go` · `GenerateOrderTokenFunc`：补"URL-safe + 不可预测"约束。
- `doc.go` · 新增 **调用方鉴权责任** 章节：明示 CreateRequest.UserID / PollStatus userID / Timeline userID 必须来自鉴权上下文；`Close` 不做归属校验；`HandleNotify` 依赖 ParseNotify 验签。
- `doc.go` · 新增 **日志与敏感信息** 章节：slog JSONHandler/TextHandler 避免日志注入 + 业务层自定义 ReplaceAttr 做脱敏。

**运维工具**：

- 新增 `scripts/check-release.sh`：CI 可接入的发版前校验，确保 drivers 的 go.mod 不含 `replace github.com/gtkit/orderflow` 指令且不 require `v0.0.0` 占位版本。

**Nitpick**：

- `crypto/rand.Read` 错误从 `_, _ = rand.Read(...)` 改为显式 `panic` 兜底，若未来 Go 工具链违反"Read 不返回 err"假设立即暴露。

**已识别但未处理**（需业务决策）：

- 【安全 I5】PII 日志脱敏：是否在 Config 加 `MaskTradeNo` / `MaskUserID` 钩子？倾向让业务层用自己的 slog.Handler.ReplaceAttr 处理，文档已说明。
- 【安全 I6】`CloseByUser(userID, orderNo)` 带归属校验变体：现通过 doc 明示调用方责任，是否需要方便 API 留给下次迭代。
- 【并发 I3】WaitGroup Add/Done 契约文档：已在 close_worker.go 对 defer 顺序加注释，是否再显式写契约注释留给下次。
- 【并发 N1】Engine 构造后不可变约定：现 doc 已提到，是否强化为"Config 不得在 New 后 mutate" 留给下次。

### Changed / Fixed (v0.5.1 企业生产级 Review 修复)

经独立 Go reviewer 审查后落地的生产级加固。审查分级 Critical / Important / Nitpick 共 24 条，本次处理 9 条高影响项：

**Critical（修复已提交）**：

- **C1 · CloseWorker 生命周期重构**：改用 `sync.WaitGroup` 精确等待在途 goroutine，替代原 pool 填槽式 drain——语义更清晰，更抗 select 随机性边界情况。`processOne` 增加 `recover` 防御，业务钩子或驱动层 panic 不会再整进程崩溃。
- **C2 · rediscache Subscription 加 `done` channel**：修复消费端不读 events buffer + 调用 `Close()` 时 forward goroutine 永久阻塞的泄漏。Close 先关 done 再关 pubsub，两层 select 都监听 done，立即解除 `events <-` 阻塞。附带回归测试 `TestStream_CloseUnblocksForwardWhenBufferFull` 保证不再复发。
- **C3 · OnPaid 幂等要求升级为强约束**：`hooks.go` 里 `OnPaidHook` 的文档从软建议升级为 3 条强制实现准则（以 orderNo 为键 / 唯一约束 / 第三方 API 调用前本地去重）。`doc.go` 补了钩子错误处理策略对照表 + 履约时序图。
- **C4 · publishStatus 双重失败 ALERT**：cache.Set 和 cache.Delete 同时失败时升级为 ERROR 日志（`orderflow: ALERT cache inconsistent`），运维可基于此关键字告警；避免缓存一致性破坏后运维无感知。

**Important（修复已提交）**：

- **I2 · CloseWorker poll 失败指数退避**：Redis / DB 故障时从 1s → 2s → 4s → ... → 30s 上限，避免日志风暴与连接池耗尽。
- **I3 · retryN ctx 感知增强**：每次 attempt 前先查 `ctx.Err()`；fn 返回 `context.Canceled` / `DeadlineExceeded` 不重试直接返回，避免浪费 attempts。
- **I6 · gormstore ColumnMap 列名白名单**：非零字段必须匹配 `^[a-zA-Z_][a-zA-Z0-9_]*$`，Store.New 时拒绝恶意配置（防御 yaml/env 注入攻击）。附带 5 个恶意输入的拒绝测试。
- **I9 · buildConfirmedNotify 零值守卫**：`query.TotalAmount` / `TradeStatus` / `Channel` 等字段只在非零值时覆盖 original，避免网关驱动 bug 把正常 notify 污染成无效值。

**Nitpick（修复已提交）**：

- **N5 · Ack 用 `context.WithoutCancel`**：保留 trace / request_id 等 value，但解耦取消信号，修复 trace 链路被切断的问题。

**架构文档增强**：`doc.go` 新增"钩子错误处理策略"对照表 + "履约时序"章节 + "生产部署清单"（含 Redis 集群 hash tag 要求、panic recover 日志关键字、driver replace 指令清理提示）。

**fallback scanner panic 防护**：CloseFallback / DeliveryFallback 的 `scan()` 单次 panic 被 recover 吞下并记 ERROR，避免单次异常永久挂死整个 scanner 主循环。

**rediszq doc.go 补充集群 hash tag 指引**：`{myapp}:order:delay_close` 样例，解释 pending + processing 双 key 必须在同一 slot 才能被 Lua 脚本 EVAL。

**已识别但暂未修复**（需设计讨论）：
- I1 Redis 集群 CROSSSLOT 需要真集群验证（miniredis 不支持）；doc 已提醒用户用 hash tag 规避。
- I4 `closeSuperseded` 网关错误是否应降级为警告——涉及业务语义取舍。
- I5 / I10 HandleNotify 对"订单消失"的处理策略需要基于业务偏好（主从延迟 vs 伪造回调）决定。
- I7 gormstore `EncodeStatus` 钩子支持其他 status 编码（非 1..6 整数）——v0.4.2+ feature。
- N7 多机 OrderNo 去重需要注入 GenerateOrderNo（doc 已提醒，但默认实现单机有效）。

### Added (v0.5.0 测试覆盖)

全仓测试补齐，现状：

| 模块 | 覆盖率 | 测试数 |
|---|---|---|
| core (`orderflow`) | 83.0% | 58 |
| `worker/` | 86.6% | 11 |
| `drivers/gormstore` | 87.4% | 18 |
| `drivers/rediscache` | 92.0% | 19 |
| `drivers/rediszq` | 73.7% | 12 |
| `drivers/paymgrgw` | 53.3%（关键判定逻辑 100%） | 6 |

所有模块 `go test -race -count=1` 通过。

核心包（`orderflow`）测试设计要点：

- **交叉验证**：每个场景同时验证 4~7 个信号（最终状态 + CAS 次数 + 日志内容 + 缓存/流事件 + 钩子调用 + 外部接口调用次数），确保锁定真实行为而非局部副作用。
- **并发竞态建模**：fakeStore 提供 `CASCloseLosesToPaidOnce` 和 `ConfirmPaidRaceOnce` 两种原子失败注入，精准重现"关闭 CAS 抢输给支付"和"确认支付 CAS 抢输给并发处理"两类真实竞态。
- **状态机全覆盖**：`HandleNotify` 12 个场景覆盖 Pending/Paid/Closed/Delivered/Completed/Cancelled 的所有分派路径，包含金额/交易号不一致、CAS 抢输重检、Closed→Paid 恢复、OnPaid 失败不向网关报错等关键路径。
- **生成器健壮性**：默认 `OrderNo` 在 1000 并发下唯一；`OrderToken` 对同输入确定、对不同输入无碰撞。

Drivers 测试要点：

- `gormstore`：基于 `github.com/glebarez/sqlite`（纯 Go SQLite 驱动）跑真实 SQL。覆盖 CAS 三件套 / FinalizePaidOrder 事务语义（FinalizeExtra 钩子错误会回滚 + 钩子能在同 tx 内读到 bill）/ ColumnMap 非默认列名 / Pending 状态下 Finalize 应失败回滚。
- `rediscache`：miniredis 集成。覆盖 TTL 按状态派发、Pending 用 expireAt + Grace、脏数据自愈、所有 Option 生效、Stream Publish→Subscribe 事件顺序、Close 幂等、非法 payload 不阻塞合法事件。
- `paymgrgw`：聚焦 `IsIgnorableCloseError` 判定逻辑（sentinel 错误识别 + 支付宝 ACQ.TRADE_NOT_EXIST 容忍 + 其他渠道不自动容忍 + 通用错误不忽略），这是这一层唯一的业务逻辑。

### Fixed

- `drivers/gormstore.FindExpiredPending`：SQL 从 `expire_at < NOW()` 改为 `expire_at < ?`（Go 侧传 `time.Now()`）。`NOW()` 是 MySQL 方言，SQLite / 测试环境下不支持；新写法可移植且便于未来注入测试时钟。

### Added (v0.4.3 rediszq driver)

- `drivers/rediszq`：基于 Redis ZSET + Lua 脚本的 `orderflow.DelayQueue` 实现。独立 Go module。
  - 代码从 `sleep_client/internal/pkg/delayqueue` 提取，现网已运行验证——同一套 pending / processing 双 ZSET + 租约模型。
  - 四个 Lua 脚本覆盖 `FetchExpired` / `ReserveExpired` / `RequeueExpired` / `Remove`，所有原子性依赖 Redis 单线程 + Lua。
  - 多实例消费安全：`ZRANGEBYSCORE + ZREM` 原子执行，同一 member 只会被一个 worker 取到。
  - `Enqueue` 用 `ZAddNX` 天然去重；`Ack` 未发生时 `RequeueExpired` 可把 lease 过期任务拉回 pending 重试。
  - 监控：`Len` / `ProcessingLen` / `ExpiredProcessingCount` / `Stats`。
  - `Option`：`WithMaxBatch` / `WithDefaultTimeout`，含 `MustNew` 便捷构造。
  - 附 12 个 miniredis 集成测试（`-race` 通过），覆盖生命周期、Ack 丢失补偿、Remove 清理 processing、批量限制、默认超时等边界。
  - 编译期断言 `var _ orderflow.DelayQueue = (*Queue)(nil)` 绑定核心接口，orderflow 契约变更会在 rediszq CI 直接失败。

### Added (v0.4.2 rediscache driver)

- `drivers/rediscache`：基于 `github.com/redis/go-redis/v9` 的 `StatusCache` 与 `StatusStream` 双实现。独立 Go module，不污染核心包依赖。
  - `NewStatusCache(rdb, opts...)`：状态缓存。沿用 sleep_client 现网的紧凑字符串编码 `"<status>:<user_id>"`，8 字节级别的 value（相比 JSON 省 3/4 空间），不引入 JSON 依赖。
  - `NewStatusStream(rdb, opts...)`：基于 Redis Pub/Sub 的状态推送。subscription 启动 forward goroutine 把 `redis.Message` 转成 `orderflow.OrderStatus` 通道。
  - TTL 按状态派发：`Pending` 对齐订单过期时间 + `PendingGrace`（默认 5min）；`Closed/Cancelled` 2min；其他活跃状态 5min。全部可通过 `WithTTL` / `WithPendingGrace` / `WithFallbackTTL` 覆盖。
  - key 前缀可通过 `WithCacheKeyPrefix` / `WithStreamKeyPrefix` 覆盖，方便多项目共用 Redis 实例。

### Added (v0.4.1 gormstore driver)

- `drivers/gormstore`：基于 GORM 的 `orderflow.Store[O]` 实现。独立 Go module，不污染核心包依赖。
  - 双泛型 `Store[O OrderSnapshot, M any]`：业务方带自己的 Order 模型 `M`，通过 `Wrap` / `BuildModel` 两个适配函数注入。
  - 内置 `OrderBill` / `OrderLog` 标准模型（用户只需指定表名），省掉多数项目的样板。
  - `ColumnMap` 可覆盖订单表关键列（`order_no` / `status` / `trade_no` 等），默认值贴合主流命名。
  - `FinalizePaidOrder` 在单事务内完成 `order Paid -> Delivered` + 账单写入 + 可选的 `FinalizeExtra` 钩子（业务侧权益发放同 tx）。
  - 全量实现 `Store[O]`：`GetByNo` / `GetByToken` / `ListByUser` / `FindPendingByUserAndProduct` / `FindExpiredPending` / `FindPaidUndelivered` / `Create` / `UpdateByOrderNo` / `CASClose` / `CASConfirmPaid` / `CASReopenPaid` / `AppendLog` / `ListLogsByOrderNo`。
  - 状态列使用 orderflow 规范整数值（1..6）。已有其他编码（如 sleep_client 的 0/10/20/...）的业务需迁移或继续使用自写 Store；`StatusValueMap` 的支持规划在后续 minor 版本。

### Added (v0.4.0 drivers 起步)

- `drivers/` 目录：每个 driver 作为**独立 Go module**，保持核心包零第三方依赖的承诺。
- `drivers/paymgrgw`：把 `github.com/gtkit/go-pay/paymgr.Manager` 适配为 `orderflow.PaymentGateway`。
  - 可选 `WithTradeType` 覆盖默认的 `TradeTypeApp`，支持 H5 / JSAPI 场景。
  - `IsIgnorableCloseError` 复用支付宝 `ACQ.TRADE_NOT_EXIST` 等生产级错误判定。
- `drivers/README.md`：记录 driver 模块化结构与清单（`paymgrgw` 已落地，`gormstore` / `rediscache` / `rediszq` 规划中）。

### Added (v0.3.0 worker 子包)

- `worker/CloseWorker`：消费 `DelayQueue`，按 `PollInterval` 节拍处理到期任务，带 `MaxWorkers` 并发控制、租约回收、Ack 独立 ctx 等生产级细节。
- `worker/CloseFallback`：周期扫描 DB 中过期 Pending 订单（`Engine.FindExpiredPending`），兜底 Redis 丢数据场景。
- `worker/DeliveryFallback`：周期扫描 Paid 未 Delivered 的订单（`Engine.FindPaidUndelivered`），对 `OnPaid` 临时失败的订单调用 `Engine.ReconcilePaid` 补偿。
- `worker/StartAll` + `worker/StartAllWithOptions`：一次性拉起三个 worker，ctx 取消后优雅退出。
- `CloseOptions` / `CloseFallbackOptions` / `DeliveryFallbackOptions` 三类独立配置，零值字段自动替换为推荐默认。

### Added (v0.2.0 核心方法)

核心 Engine 方法（可供上游集成）：

- `Engine.Create`：创建订单，含"同用户+同商品 Pending 单"的复用与取代分支，入延时队列失败时的自保护关闭，以及 OnCreated 钩子触发。
- `Engine.PollStatus`：缓存优先的状态查询，miss 时回源 DB 并回填缓存，携带 UserID 做归属校验。
- `Engine.Timeline`：订单状态流水查询。
- `Engine.ListUserOrders`：按 UserID 列表查询。
- `Engine.Subscribe`：透传到 StatusStream driver 的订阅接口。
- `Engine.HandleNotify`：支付回调处理，覆盖 Pending / Paid / Closed / Delivered 等全状态分派，包含金额/交易号异常告警、CAS 抢跑重检、幂等重试、"已关闭又被支付成功"恢复路径。
- `Engine.Close`：支付超时关闭（调用网关 CloseOrder + CAS 推进 Closed，带 3 次重试和可忽略错误容忍）。
- `Engine.ReconcilePaid`：对 `Paid 但未 Delivered` 订单做履约补偿。
- `Engine.FindExpiredPending` / `FindPaidUndelivered`：passthrough 方法，供 fallback worker 使用。

### Changed

- `Store.FinalizePaidOrder` 签名从 `(ctx, order, bill, extra map[string]any)` 精简为 `(ctx, order, bill)`。driver 如需持久化业务权益快照，从 `order.Extra()` 读取，保证订单 + 账单 + 权益在同一事务。

### Internals

- 新增 `retryN` 辅助工具（stdlib-only，固定次数 + 固定间隔），用于网关 CloseOrder / QueryOrder 场景的瞬时重试。
- `publishStatus` / `appendLog` / `recordAnomaly` / `resolveChannelOf` / `buildNotifyURLOf` / `isReusableOf` 等私有助手集中在 `helpers.go`，保证核心方法路径清晰。

## [0.1.0] - 待正式 tag

### Added

- 落地核心包骨架：
  - `OrderSnapshot` 接口与 `OrderStatus` 枚举（含合法跃迁表）。
  - `OrderSpec` / `ProductInfo` / `BillSpec` 中性载荷类型。
  - `Store[O]` / `PaymentGateway` / `DelayQueue` / `StatusCache` / `StatusStream` 五个能力接口。
  - 业务钩子函数类型：`OnCreated` / `OnPaid` / `OnDelivered` / `OnClosed` / `OnReopened` / `OnSuperseded` / `OnAnomaly`。
  - `ClosedReason` / `AnomalyKind` 事件枚举；sentinel 错误集合。
  - 默认 `OrderNo` / `OrderToken` 生成器（stdlib-only）。
  - `Engine[O]` 构造函数 `New`，含依赖与参数的合法性校验。
- 项目初始化：`README.md`、`CHANGELOG.md`、包级 `doc.go`。
