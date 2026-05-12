# orderflow 生产接入 Checklist

> 本文件是接入方**首次部署**到生产环境前的硬门禁清单。每一项都来自真实事故或核心包
> 无法在编译期/运行时自动强制的契约。漏掉任何一项都可能导致**资损 / 数据不一致 /
> 监控盲区**。本 checklist 与散落在 [`doc.go`](./doc.go) 生产清单、[`README.md`](./README.md)
> 部署章节、[`refund_observability.md`](./refund_observability.md) 退款约定**互为
> 索引**——以本文件为单一入口。

接入前请按以下顺序逐项确认。每个 `[ ]` 都必须能给出明确"是"的依据。

---

## 1. 安全（绝对不能漏，漏一项即资损风险）

- [ ] **`PaymentGateway.ParseNotify` / `RefundGateway.ParseRefundNotify` 必须做完整签名校验**。
  核心包**信任** driver 的 ParseNotify 输出已经验签，自身不会再次验签。详见
  [`gateway.go`](./gateway.go) 与 [`refund_gateway.go`](./refund_gateway.go) 的"安全契约"
  章节。
  - 自研 driver 接入前必须人工 review 验签逻辑；
  - 使用 `drivers/paymgrgw`（基于 `gtkit/go-pay`）时上游已实现验签，但仍建议跑沙箱测试用
    伪造 body 验证返回 error。

- [ ] **`CreateRequest.UserID` 必须来自 JWT / Session 等可信身份上下文**，不得从 HTTP body / query
  / header 直接读取。攻击者传他人 UserID 会通过 `FindPendingByUserAndProduct` 找到受害者
  Pending 订单并经 `closeSuperseded` 关闭（DoS）。

- [ ] **`PollStatus` / `Timeline` / `CloseByUser` / `CancelByUser` 的 `userID` 参数同样来自可信
  上下文**——Engine 内部据此做归属校验，业务层透传等于校验形同虚设。

- [ ] **`Close(orderNo)` 不做 UserID 校验**——仅供 worker / admin 调用，**禁止**绑到对外 HTTP
  路由。用户接口对照表：
  - 用户关闭**已过期** Pending 订单 → `CloseByUser`（绕过 CloseFallback 调度，立即收尾）
  - 用户**取消**未过期 Pending 订单 → `CancelByUser`（推到 `StatusCancelled`，与系统型关闭区分）
  - 后台 / 风控强制关单 → `CloseByAdmin`（绕过 ExpireAt 守卫，前置 RBAC 鉴权）

- [ ] **Logger 适配器必须做敏感字段过滤**——`OrderToken` / `TradeNo` / `UserID` / `Amount` 会
  通过结构化日志输出。建议在 Logger 实现内部统一脱敏（如 `sk-****abcd` / `Bearer ****`），
  不要散落在调用点。

- [ ] **日志收集链路（Kafka / ES）的访问权限按最小原则控制**——日志里的字段在 PCI-DSS / 个人
  信息保护法等框架下可能属于敏感数据。

---

## 2. 业务幂等（OnPaid 双发是最高频事故）

- [ ] **`OnPaid` 钩子必须幂等**——网关重传 / `retryFinalizeForPaid` 重入 / `DeliveryFallback`
  补偿都可能多次触发。漏幂等 = 双倍发券 / 重复发货 / 积分多发。
  - **未配置 OnPaid 幂等保护时 `Engine.New` 会输出 Warn 启动日志**——务必看见这条 Warn 立即处理；
  - 推荐使用 [`drivers/rediscache.IdempotentOnPaidViaRedis`](./drivers/rediscache/locker.go)
    包装 OnPaid（Redis SETNX 幂等标记，自动处理"先成功后重传"场景）；
  - 业务自行幂等的常见方式：业务侧维护幂等表 `business_idem (idem_key VARCHAR PRIMARY KEY, ...)`，
    OnPaid 内部 `INSERT IGNORE`；
  - 确认幂等已就位后可设 `Config.SkipOnPaidIdempotencyWarn=true` 静音启动 Warn。

- [ ] **`OnReopened` 钩子内禁止做"发权益"类副作用**——`OnReopened` 触发后 Engine 会立刻调
  `OnPaid + OnDelivered`，重复发放会导致双倍。`OnReopened` 仅做事件通知 / 审计 / 告警。详见
  [`hooks.go::OnReopenedHook`](./hooks.go) GoDoc。

- [ ] **退款编排的反向核销逻辑必须幂等** —— 同一 `refund_id` 多次调用 `revokeBenefits` 结果一致。
  详见 [`examples/refund_quickstart/main.go`](./examples/refund_quickstart/main.go) 示例。

- [ ] **`Store.AppendLog` 失败不阻断主流程但必须告警** —— 流水写入失败时 Engine 通过
  `AnomalyAppendLogFailed` 上报，业务侧必须配告警（合规风险）。

---

## 3. 基础设施（DB / Redis / 队列）

- [ ] **DB 必备索引**（详见 [`drivers/gormstore/doc.go`](./drivers/gormstore/doc.go) "必备索引清单"）：
  - `UNIQUE INDEX uk_order_no ON orders (order_no)`
  - `UNIQUE INDEX uk_order_token ON orders (order_token)`
  - `INDEX idx_status_expire_at ON orders (status, expire_at)` —— `FindExpiredPending` 全表扫保护
  - `INDEX idx_status_paid_at ON orders (status, paid_at)` —— `FindPaidUndelivered` 全表扫保护
  - `INDEX idx_user_product_status ON orders (user_id, product_id, status)` —— `FindPendingByUserAndProduct`
  - `INDEX idx_user_id ON orders (user_id)` —— `ListByUser`

- [ ] **"一用户一商品一 Pending" DB 兜底唯一索引（强烈推荐）**：
  ```sql
  -- PostgreSQL
  CREATE UNIQUE INDEX uk_user_product_pending ON orders (user_id, product_id) WHERE status = 0;
  -- MySQL 8.0+
  ALTER TABLE orders ADD UNIQUE INDEX uk_pending_user_product
    ((CASE WHEN status=0 THEN CONCAT(user_id,'-',product_id) ELSE NULL END));
  ```
  与"前端 debounce + 应用层 `Locker` + DB 兜底"三层防御组合使用。

- [ ] **Redis 集群部署：`rediszq` driver 的 key 必须用 hash tag** —— Queue 内部同时操作 `key`
  和 `key + ":processing"`，未加 hash tag 会触发 `CROSSSLOT` 错误让所有操作失败。详见
  [`drivers/rediszq/doc.go`](./drivers/rediszq/doc.go)：
  ```go
  q, err := rediszq.New(rdb, "{myapp}:order:delay_close")
  ```

- [ ] **Redis / DB 连接池容量配置**：核心包不限定，业务方按渠道 RTT × QPS 估算。一般建议
  Redis maxIdle ≥ 单实例 QPS × 平均响应时间（s）。

- [ ] **时区显式配置**：`Config.Timezone="Asia/Shanghai"` 等 IANA 名称——否则日志 / 流水
  `created_at` 用 `time.Local` 容易踩跨机房时区不一致的坑。

---

## 4. 配置选择（生产环境推荐 vs 默认值的差异）

- [ ] **`CloseSupersededPolicy = SupersededDegraded`**（**强烈推荐**生产配置）。
  默认 `SupersededStrict` 是 v1.0 向后兼容值，网关抖动时会阻塞用户下新单。Degraded 模式下
  网关 close 失败会本地继续 + 触发 `OnSupersededGatewayCloseFailed` hook 让业务方做自定义
  补救（详见 [`hooks.go`](./hooks.go)），由 CloseFallback 兜底网关侧旧单清理。
  **代价**：极短窗口内"本地 Closed 但网关还认为 Pending"——若用户在此窗口完成支付，
  `handleClosedPaidNotify` 自动恢复。

- [ ] **`Locker` 必须注入**（生产环境）—— 配合 DB 唯一索引做并发创建防御，避免同用户同商品
  瞬间下出多个 Pending 单。推荐 `drivers/rediscache` 内的 Locker 实现。

- [ ] **`CreateLockTTL` 合理设置** —— 默认 `DefaultCreateLockTTL=10s`，必须 > Create 实际耗时
  上限（含 `UnifiedOrder` 网络 RTT），否则锁提前释放导致并发 bypass。

- [ ] **`OrderExpire` 与产品支付页 UI 一致** —— 默认 30 min，业务侧支付页倒计时应与此值对齐。

- [ ] **注入完整可观测性套件**：`Observer`（Prometheus / OpenTelemetry 适配器，**禁止**接业务事件
  总线）+ `Logger`（结构化 + 敏感字段过滤）+ 实现 `Healther` 接口的全部依赖
  （Store / Cache / Stream / DelayQueue / Locker / PaymentGateway）。

---

## 5. 监控告警规则（生产事故的最后一道防线）

Engine 通过 `EventAnomaly` Observer 事件上报 13 类异常。**全部都值得配告警**——按优先级：

### 5.1 P0 级（必须配告警，事故级）

- [ ] **`AnomalyDeliveryFailed`** —— `OnPaid` 或 `FinalizePaidOrder` 失败，订单卡在 Paid
  未 Delivered。`DeliveryFallback` worker 会重试，但**长期失败 = 业务侧权益未发**。
  Prometheus 告警规则示例：
  ```promql
  # 同一 orderNo 5min 内 delivery_failed ≥ 3 次 → P0 告警人工介入
  sum by (order_no) (
    increase(orderflow_anomaly_total{kind="delivery_failed"}[5m])
  ) >= 3
  ```

- [ ] **`AnomalyAmountMismatch`** —— notify 金额 ≠ 订单金额。可能是渠道侧 bug 或攻击。
  Engine 不推进状态，需人工介入。

- [ ] **`AnomalyOrderDisappeared`** —— CAS 失败后 recheck 发现订单消失。数据完整性事故。

- [ ] **`AnomalyMalformedPaidNotify`** —— Paid 回调缺 `TransactionID` 或 `TotalAmount<=0`。
  合法渠道不会出现，触发即意味着上游签名校验缺陷 / 伪造请求 / driver bug。

### 5.2 P1 级（运营观察 + 阈值告警）

- [ ] **`AnomalyTradeNoMismatch`** —— 同一订单不同 notify 出现不同 tradeNo。Engine 阻断 finalize，
  Bill 表只记录第一条 trade_no。

- [ ] **`AnomalyPaidOnClosed`** —— Closed 订单收到 paid notify 但网关查询非 Paid。

- [ ] **`AnomalyDelayQueueCleanupFailed`** —— 终态推进后清理延时队列残留失败（Queue 故障）。
  Redis / 队列层可用性告警。

- [ ] **`AnomalyGatewayQueryFailed`** —— 查询网关 3 次重试全失败。网关侧告警。

- [ ] **`AnomalySupersededGatewayCloseFailed`** —— `SupersededDegraded` 模式下网关旧单关闭
  失败。业务方应已实现 `OnSupersededGatewayCloseFailed` hook 推到自定义重试队列；本告警
  作为兜底监控。

- [ ] **`AnomalyAppendLogFailed`** —— 流水写入失败（合规风险）。

### 5.3 退款相关（业务方主动 emit，核心包不主动 emit）

- [ ] **`AnomalyRefundGatewayFailed`** —— 退款网关多次重试失败。
- [ ] **`AnomalyRefundDrift`** —— 异步通知与 QueryRefund 状态冲突，需人工对账。

详见 [`refund_observability.md`](./refund_observability.md)。

### 5.4 额外的"独立于 Engine"告警（强烈建议）

- [ ] **订单 Paid 但未 Delivered 超过 X 分钟**——业务侧基于 `orders` 表自查，独立于 worker
  内部指标，作为最后一道防线：
  ```sql
  SELECT count(*) FROM orders WHERE status = 10 AND paid_at < NOW() - INTERVAL 10 MINUTE
  ```

- [ ] **Fallback scanner 心跳告警**——Observer 暴露 `CloseFallback` / `DeliveryFallback` 的扫描
  耗时与处理量，配置"超过 N 分钟无扫描"告警（worker 假死 = 多条 anomaly 链路失去兜底）。

- [ ] **"orderflow: ALERT ..." 日志关键字告警**——Engine 在所有严重异常路径都打 ERROR 级别
  `orderflow: ALERT ...` 前缀，配 ELK / Loki 关键字告警。

---

## 6. 上线前一次性自检

```bash
# 1. 编译 + lint
GOWORK=off go vet ./...
bash scripts/lint-all.sh                # 多 module（drivers 是独立 module，根 ./... 扫不到）

# 2. 测试
GOWORK=off go test -race -count=1 -timeout=5m ./...
GOWORK=off go test -bench=. -benchmem -count=3 ./...
GOWORK=off go test -coverprofile=coverage.out ./...

# 3. 发版前完整门禁
bash scripts/check-release.sh           # driver readiness + lint + 模块发版审计
```

部署清单确认：

- [ ] `worker.StartAll(ctx, engine)` **必须**随服务启动——`CloseWorker` + `CloseFallback` +
  `DeliveryFallback` 三个 worker 缺一不可。
- [ ] K8s readiness probe 调 `Engine.Healthy(ctx)`——会探测所有实现 `Healther` 接口的依赖。
- [ ] `CHANGELOG.md` 有对应版本号区段；driver 的 `go.mod` 不能有未删除的 `replace` 指令。
- [ ] 沙箱环境完成端到端测试：下单 → 支付（模拟）→ 履约 → 关闭 / 取消 / 退款。

---

## 7. 相关文档索引

| 文档 | 用途 |
|---|---|
| [`doc.go`](./doc.go) | 包级 GoDoc：完整设计原理、状态机、安全契约、生产清单 |
| [`README.md`](./README.md) | 快速接入示例 + API 概览 + 钩子表 + 错误处理流程图 |
| [`refund_observability.md`](./refund_observability.md) | 退款编排的 Observer emit 时机与 attribute schema |
| [`drivers/gormstore/doc.go`](./drivers/gormstore/doc.go) | DB 索引清单、列映射、事务边界 |
| [`drivers/rediszq/doc.go`](./drivers/rediszq/doc.go) | Redis 延时队列：hash tag、租约、Ack |
| [`drivers/rediscache/doc.go`](./drivers/rediscache/doc.go) | StatusCache / Locker / IdempotentOnPaidViaRedis |
| [`drivers/paymgrgw/doc.go`](./drivers/paymgrgw/doc.go) | 支付 + 退款网关 driver |
| [`CHANGELOG.md`](./CHANGELOG.md) | 版本变更（与 SemVer tag 强绑定） |
