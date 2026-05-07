// 示例：业务方如何基于 orderflow.RefundGateway 自行编排退款流程。
//
// 本文件可独立 `go build`，但内部使用 sql.DB 占位 + 简化的伪 SQL，目的是展示
// **企业生产级编排骨架**而非可直接运行的服务：
//
//   - 事务边界（网关 IO 不持锁）
//   - 幂等性（PK 冲突识别 → reconcile 兜底；CAS 防重放）
//   - 状态机正确性（按 RefundResponse.Status.IsTerminal() 分支处理）
//   - 反向核销失败兜底（outbox 模式重试，避免静默丢核销）
//
// 业务方接入时按自己的 ORM / DB 类型替换数据访问；审批工作流、金额计算、
// 反向核销策略由业务方自定义。
//
// 关键模式（务必照搬）：
//
//  1. 尝试 INSERT pending 记录；PK 冲突说明是重试场景，走 reconcile 拉真实状态
//  2. 事务外调 Gateway.Refund —— 网络 IO 不能持锁
//  3. err != nil 时按 IsIgnorableRefundError / 主动 Query 兜底
//  4. resp.Status.IsTerminal() 决定本地状态机走向：
//     终态 → markResolved 触发反向核销
//     中间态 → 推进到 status=resp.Status 等异步通知
//  5. CAS UPDATE，WHERE status NOT IN ('succeeded', 'failed') 防终态被覆盖
//  6. CAS winner 才触发反向核销；失败入 outbox 队列重试，**不**仅日志
//  7. 异步通知路径：ParseRefundNotify → 业务校验（金额 / channel 一致性）→ CAS UPDATE → AckRefundNotify
//
// # 参考 schema（业务方按真实 DB 调整）
//
//	CREATE TABLE business_refund_records (
//	    id                VARCHAR(64) NOT NULL PRIMARY KEY,    -- OutRefundNo 业务幂等键
//	    order_no          VARCHAR(64) NOT NULL,                -- 原支付订单号
//	    channel           VARCHAR(32) NOT NULL,                -- 原支付渠道（必持久化，防错配）
//	    amount            BIGINT      NOT NULL,                -- 审批后的退款金额（分）
//	    total_amount      BIGINT      NOT NULL,                -- 原订单总金额（分）
//	    status            VARCHAR(16) NOT NULL DEFAULT 'pending',
//	                                          -- 5 取值：pending/processing/succeeded/failed/unknown
//	                                          -- 用 ENUM 时务必含 unknown，否则 mapRefundStatus
//	                                          -- 返回 unknown 时 INSERT/UPDATE 失败
//	    gateway_refund_id VARCHAR(64) NOT NULL DEFAULT '',     -- 渠道侧退款单号（如有）
//	    succeeded_at      DATETIME(3) NULL,                    -- **必须允许 NULL**——仅 succeeded
//	                                                              终态时填，其他状态零值导致 NOT NULL
//	                                                              列拒绝（MySQL 严格模式）
//	    last_error        TEXT        NULL,                    -- 最近一次失败的错误信息（可选）
//	    requested_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
//	    updated_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
//	                                          ON UPDATE CURRENT_TIMESTAMP(3),
//	    INDEX idx_order_no (order_no),
//	    INDEX idx_status_requested (status, requested_at)
//	);
//
//	-- 反向核销失败重试队列（详见 README "反向核销失败的兜底" 章节）
//	CREATE TABLE business_revoke_retry_queue (
//	    refund_id        VARCHAR(64) NOT NULL PRIMARY KEY,
//	    last_error       TEXT        NOT NULL,
//	    retry_count      INT         NOT NULL DEFAULT 0,
//	    next_attempt_at  DATETIME(3) NOT NULL,
//	    created_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
//	    INDEX idx_next_attempt (next_attempt_at)
//	);
//
//	-- 累加防超额（详见 README "并发退款的累加校验" 章节）
//	ALTER TABLE orders ADD COLUMN refunded_amount BIGINT NOT NULL DEFAULT 0;
//	ALTER TABLE orders ADD CONSTRAINT chk_refund_not_overflow
//	    CHECK (refunded_amount <= pay_amount);
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gtkit/orderflow"
)

// Application 业务侧的退款申请单（含审批后的最终金额）。
type Application struct {
	ID          string            // 业务侧主键，作为 OutRefundNo 幂等键
	OrderNo     string            // 原支付订单号
	Channel     orderflow.Channel // 原支付渠道（业务方必须持久化以避免错配）
	OrderAmount int64             // 原订单金额（分）
	Amount      int64             // 审批后最终退款金额（分），可能 < OrderAmount
	Reason      string            // 退款原因
}

// RefundService 业务侧退款服务。库内不引入此类型，由业务方自行实现。
type RefundService struct {
	db      *sql.DB
	gateway orderflow.RefundGateway
}

// Apply 处理"审批通过"事件：发起退款 → CAS 落终态。
//
// 进入条件：业务侧审批工作流已完成，a.Amount 是审批后的最终金额。
// 调用方应保证不会用同一个 a.ID 重入（除非是重试场景，本函数会处理）。
func (s *RefundService) Apply(ctx context.Context, a Application) error {
	// Step 1：尝试 INSERT pending 记录
	//
	// PK 冲突 → 此 ApplicationID 已发起过退款（典型是客服重试场景）：
	// 走 reconcile 拉渠道侧真实状态推进本地状态机，**不**重新发起退款。
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO business_refund_records
		   (id, order_no, channel, amount, total_amount, status, requested_at)
		 VALUES (?, ?, ?, ?, ?, 'pending', ?)`,
		a.ID, a.OrderNo, a.Channel, a.Amount, a.OrderAmount, time.Now())
	if err != nil {
		if isPKConflict(err) {
			return s.reconcile(ctx, a)
		}
		return fmt.Errorf("insert refund record: %w", err)
	}

	// Step 2：事务外调网关
	resp, err := s.gateway.Refund(ctx, a.Channel, orderflow.RefundRequest{
		OutTradeNo:   a.OrderNo,
		OutRefundNo:  a.ID,
		RefundAmount: a.Amount,
		TotalAmount:  a.OrderAmount,
		Reason:       a.Reason,
		NotifyURL:    "https://example.com/refund-notify",
	})
	if err != nil {
		// 已识别的渠道幂等错误 → 走 Query 路径
		if s.gateway.IsIgnorableRefundError(a.Channel, err) {
			return s.reconcile(ctx, a)
		}
		// 临时缓解：未识别错误也尝试一次主动 Query 兜底——
		// 真的是请求失败时 Query 也会失败，回退到原始错误；
		// 是渠道侧已收到但 driver 不识别错误码时，Query 能拿到真实状态推进。
		if qres, qerr := s.gateway.QueryRefund(ctx, a.Channel, a.ID); qerr == nil && qres.Status != "" {
			return s.markResolved(ctx, a.ID, qres.Status, qres.GatewayRefundID, qres.SucceededAt)
		}
		// 真错误向上抛；pending 记录留在 DB 由客服重试或对账 worker 兜底
		return fmt.Errorf("gateway refund: %w", err)
	}

	// Step 3：按 resp.Status 决定本地状态机
	//
	// **必须用 IsTerminal() 判断**，不要按渠道名硬编码——driver 的 syncRefundStatus
	// 是基于已知行为模式的启发式默认值，未来渠道行为变化时仍要保持业务正确性。
	if resp.Status.IsTerminal() {
		return s.markResolved(ctx, a.ID, resp.Status, resp.GatewayRefundID, time.Now())
	}
	// 中间态：仅记录 GatewayRefundID + 推进到 processing，等异步回调
	_, err = s.db.ExecContext(ctx,
		`UPDATE business_refund_records SET gateway_refund_id = ?, status = ?
		 WHERE id = ? AND status = 'pending'`,
		resp.GatewayRefundID, string(resp.Status), a.ID)
	return err
}

// reconcile 主动对账：调 QueryRefund 拿真实状态后 CAS 推进。
//
// 触发场景：(1) Apply 撞 PK 冲突（重试）；(2) Refund 拿到 IsIgnorableRefundError；
// (3) 业务侧定时对账 worker 扫描长时间 pending 记录。
func (s *RefundService) reconcile(ctx context.Context, a Application) error {
	res, err := s.gateway.QueryRefund(ctx, a.Channel, a.ID)
	if err != nil {
		if errors.Is(err, orderflow.ErrRefundNotFound) {
			// 渠道侧从未受理——业务方决定是回滚 pending 还是人工介入。
			// 典型场景：上次 Refund 调用网络层就失败，渠道侧根本没收到请求。
			log.Printf("refund %s not found at gateway, manual intervention required", a.ID)
			return err
		}
		return err
	}
	return s.markResolved(ctx, a.ID, res.Status, res.GatewayRefundID, res.SucceededAt)
}

// HandleNotify 处理网关异步通知。
func (s *RefundService) HandleNotify(ctx context.Context, ch orderflow.Channel, w http.ResponseWriter, r *http.Request) {
	notify, err := s.gateway.ParseRefundNotify(ctx, ch, r)
	if err != nil {
		// 验签失败——返回非 200 让网关重发或人工排查；不要 ack
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 业务侧二次校验（关键安全防线）：即便 driver 已验签，业务方也应校验
	// notify 里的 OutRefundNo / channel / RefundAmount 与本地 record 一致——
	// 防止伪造攻击、防止 channel 错配、防止 driver 实现 bug。
	if err := s.verifyNotify(ctx, ch, notify); err != nil {
		log.Printf("notify verify failed for %s: %v", notify.OutRefundNo, err)
		http.Error(w, "verify failed", http.StatusBadRequest)
		return
	}

	if err := s.markResolved(ctx, notify.OutRefundNo, notify.Status,
		notify.GatewayRefundID, notify.SucceededAt); err != nil {
		// DB 写失败：不 ack，让网关重发
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 落库成功才 ack
	if err := s.gateway.AckRefundNotify(ch, w); err != nil {
		log.Printf("ack refund notify: %v", err)
	}
}

// verifyNotify 业务侧二次校验：channel / amount 一致性。
func (s *RefundService) verifyNotify(ctx context.Context, ch orderflow.Channel, n orderflow.RefundNotifyResult) error {
	var (
		localChannel string
		localAmount  int64
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT channel, amount FROM business_refund_records WHERE id = ?`,
		n.OutRefundNo).Scan(&localChannel, &localAmount)
	if err != nil {
		return fmt.Errorf("load record: %w", err)
	}
	if orderflow.Channel(localChannel) != ch {
		return fmt.Errorf("channel mismatch: notify=%s local=%s", ch, localChannel)
	}
	if n.RefundAmount != localAmount {
		return fmt.Errorf("amount mismatch: notify=%d local=%d", n.RefundAmount, localAmount)
	}
	return nil
}

// markResolved 是 CAS 防重放的核心：affected==1 时才触发反向核销。
//
// WHERE status NOT IN ('succeeded', 'failed') 表示"只要不是终态都允许覆盖"——
// pending / processing / unknown 三个非终态都可推进，避免业务方卡在中间状态：
//
//   - pending → processing/succeeded/failed/unknown：合法（首次推进）
//   - processing → succeeded/failed/unknown：合法（异步通知 / Query 推进）
//   - unknown → processing/succeeded/failed：合法（人工介入或 Query 拉到真实状态后推进）
//   - succeeded / failed：终态，CAS 不允许覆盖（防止误操作回退）
//
// 重复回调防护：终态后 affected=0；中间态间互转 affected=1 但 status != Succeeded
// 时不会触发反向核销，行为正确。
func (s *RefundService) markResolved(
	ctx context.Context,
	refundID string,
	status orderflow.RefundTradeStatus,
	gatewayRefundID string,
	succeededAt time.Time,
) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE business_refund_records
		   SET status = ?, gateway_refund_id = ?, succeeded_at = ?
		 WHERE id = ? AND status NOT IN ('succeeded', 'failed')`,
		status, gatewayRefundID, succeededAt, refundID)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		// 已处理过的回调（重复回调或并发竞争失败者）——不再触发反向核销
		return nil
	}

	if status != orderflow.RefundTradeStatusSucceeded {
		// 终态非 succeeded（failed）：不需要反向核销
		return nil
	}

	// CAS winner + status=succeeded：触发业务侧反向核销
	//
	// **关键**：反向核销失败**不能仅日志**——退款款项已经从渠道侧扣减发出，
	// 业务侧权益必须回退，否则用户白嫖造成资损。推荐 outbox 模式：
	// 失败时入"反向核销重试队列"，独立 worker 周期重试 + 超过阈值告警人工介入。
	if err := s.revokeBenefits(ctx, refundID); err != nil {
		if outboxErr := s.enqueueRevokeRetry(ctx, refundID, err); outboxErr != nil {
			// outbox 也失败：本地数据严重不一致风险，必须 CRITICAL 告警 + 强制人工介入
			log.Printf("CRITICAL refund %s revoke + outbox both failed: revokeErr=%v outboxErr=%v",
				refundID, err, outboxErr)
			return fmt.Errorf("post-refund revoke unrecoverable: %w", err)
		}
		// 入 outbox 成功：让独立 worker 重试；本路径返回成功（CAS 已落，渠道侧确实退款了）
		log.Printf("refund %s revoke deferred to outbox: %v", refundID, err)
	}
	return nil
}

// revokeBenefits 业务方自定义的反向核销逻辑（这里仅作占位）。
//
// 真实业务方实现应该是幂等的——同一个 refundID 多次调用结果一致（已核销则跳过）。
func (s *RefundService) revokeBenefits(_ context.Context, refundID string) error {
	log.Printf("revoke benefits for refund %s (business-specific, must be idempotent)", refundID)
	return nil
}

// enqueueRevokeRetry 把反向核销失败的退款单入 outbox 重试队列。
//
// 业务方自定义实现可以是 DB 表 / Redis Stream / 外部 MQ 等；独立的 worker
// 进程定期扫描 next_attempt_at 到期的记录重试 revokeBenefits，超过 N 次后告警。
func (s *RefundService) enqueueRevokeRetry(ctx context.Context, refundID string, lastErr error) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO business_revoke_retry_queue
		   (refund_id, last_error, retry_count, next_attempt_at)
		 VALUES (?, ?, 0, ?)`,
		refundID, lastErr.Error(), time.Now().Add(time.Minute))
	return err
}

// isPKConflict 业务方按自己的 DB 驱动识别 PK 冲突（典型 MySQL 1062 / PostgreSQL 23505）。
//
// 这里用 strings.Contains 占位演示——真实代码按 driver-specific 类型断言。例如：
//
//	MySQL（go-sql-driver/mysql）：
//	    var me *mysql.MySQLError
//	    return errors.As(err, &me) && me.Number == 1062
//
//	PostgreSQL（pgx/jackc）：
//	    var pe *pgconn.PgError
//	    return errors.As(err, &pe) && pe.Code == "23505"
//
//	GORM：
//	    return errors.Is(err, gorm.ErrDuplicatedKey)
func isPKConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Duplicate entry") || // MySQL
		strings.Contains(msg, "duplicate key value") || // PostgreSQL
		strings.Contains(msg, "UNIQUE constraint failed") // SQLite
}

func main() {
	// 真实代码：业务方自己构造 sql.DB + paymgrgw.New(paymgr.NewManager())。
	// 本 main 仅占位让 go build 通过。
	_ = (&RefundService{}).Apply
	_ = (&RefundService{}).HandleNotify
	_ = (&RefundService{}).reconcile
	_ = (&RefundService{}).markResolved
	_ = (&RefundService{}).revokeBenefits
	_ = (&RefundService{}).verifyNotify
	_ = (&RefundService{}).enqueueRevokeRetry
	_ = isPKConflict
	fmt.Println("refund_quickstart compiled successfully")
}
