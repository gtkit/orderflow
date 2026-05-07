// 示例：业务方如何基于 orderflow.RefundGateway 自行编排退款流程。
//
// 本文件可独立 `go build`，但内部使用 sql.DB 占位 + 简化的伪 SQL，目的是展示
// 编排骨架（事务边界 / CAS 模式 / 异步通知去重 / 反向核销）而非真实可运行的服务。
//
// 业务方接入时按自己的 ORM / DB 类型替换数据访问；审批工作流、金额计算、
// 反向核销策略由业务方自定义。
//
// 关键模式（务必照抄）：
//
//   1. 事务 A 内 INSERT 退款记录 status=pending（业务自定义表）
//   2. 事务外调 Gateway.Refund —— 网络 IO 不能持锁
//   3. 视返回 / IsIgnorableRefundError 走不同分支
//   4. 事务 B 内 CAS UPDATE，WHERE status IN ('pending','processing') 防重放
//   5. CAS winner 才触发反向核销（OnRefunded 业务侧实现）
//   6. 异步通知路径：ParseRefundNotify → CAS UPDATE → 反向核销 → AckRefundNotify
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gtkit/orderflow"
)

// Application 业务侧的退款申请单（含审批后的最终金额）。
type Application struct {
	ID          string             // 业务侧主键，作为 OutRefundNo 幂等键
	OrderNo     string             // 原支付订单号
	Channel     orderflow.Channel  // 原支付渠道
	OrderAmount int64              // 原订单金额（分）
	Amount      int64              // 审批后最终退款金额（分），可能 < OrderAmount
	Reason      string             // 退款原因
}

// RefundService 业务侧退款服务。库内不引入此类型，由业务方自行实现。
type RefundService struct {
	db      *sql.DB
	gateway orderflow.RefundGateway
}

// Apply 处理"审批通过"事件：发起退款 → CAS 落终态。
func (s *RefundService) Apply(ctx context.Context, a Application) error {
	// Step 1：事务 A 内 INSERT pending 记录（业务侧表 / 字段自定义）
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO business_refund_records
		   (id, order_no, channel, amount, total_amount, status, requested_at)
		 VALUES (?, ?, ?, ?, ?, 'pending', ?)`,
		a.ID, a.OrderNo, a.Channel, a.Amount, a.OrderAmount, time.Now()); err != nil {
		// PK 冲突说明此 ApplicationID 已发起过退款——业务方按幂等语义处理（继续走 Gateway.Refund）
		// 这里简化为直接返回；生产代码应区分 PK 冲突 vs 其他错误。
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
		// IsIgnorableRefundError 命中说明渠道侧已处理过，走 Query 路径拿真实状态
		if s.gateway.IsIgnorableRefundError(a.Channel, err) {
			return s.reconcile(ctx, a)
		}
		// 其他错误向上抛，pending 记录留在 DB 由客服重试
		return fmt.Errorf("gateway refund: %w", err)
	}

	// Step 3：记录网关返回的退款单号；最终态等异步通知或主动 Query 推进
	_, err = s.db.ExecContext(ctx,
		`UPDATE business_refund_records SET gateway_refund_id = ?, status = 'processing'
		 WHERE id = ? AND status = 'pending'`,
		resp.GatewayRefundID, a.ID)
	return err
}

// reconcile 主动对账：调 QueryRefund 拿真实状态后 CAS 推进。
func (s *RefundService) reconcile(ctx context.Context, a Application) error {
	res, err := s.gateway.QueryRefund(ctx, a.Channel, a.ID)
	if err != nil {
		if errors.Is(err, orderflow.ErrRefundNotFound) {
			// 渠道侧从未受理——业务方决定是回滚 pending 还是人工介入
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

	if err := s.markResolved(ctx, notify.OutRefundNo, notify.Status, notify.GatewayRefundID, notify.SucceededAt); err != nil {
		// DB 写失败：不 ack，让网关重发
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 落库成功才 ack
	if err := s.gateway.AckRefundNotify(ch, w); err != nil {
		log.Printf("ack refund notify: %v", err)
	}
}

// markResolved 是 CAS 防重放的核心：affected==1 时才触发反向核销。
//
// WHERE status IN ('pending','processing') 是必须的——避免重复回调让 OnRefunded 触发多次。
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
		 WHERE id = ? AND status IN ('pending', 'processing')`,
		status, gatewayRefundID, succeededAt, refundID)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		// 已处理过的回调——不再触发反向核销
		return nil
	}

	// CAS winner：触发业务侧反向核销（虚拟商品下架 / 会员权益回退 / 课时核销 等）
	if status == orderflow.RefundTradeStatusSucceeded {
		if err := s.revokeBenefits(ctx, refundID); err != nil {
			// 反向核销失败仅日志；业务方自定义重试 / 告警策略
			log.Printf("revoke benefits for refund %s: %v", refundID, err)
		}
	}
	return nil
}

// revokeBenefits 业务方自定义的反向核销逻辑（这里仅作占位）。
func (s *RefundService) revokeBenefits(_ context.Context, refundID string) error {
	log.Printf("revoke benefits for refund %s (business-specific)", refundID)
	return nil
}

func main() {
	// 真实代码：业务方自己构造 sql.DB + paymgrgw.New(paymgr.NewManager())。
	// 本 main 仅占位让 go build 通过。
	_ = (&RefundService{}).Apply
	_ = (&RefundService{}).HandleNotify
	_ = (&RefundService{}).reconcile
	_ = (&RefundService{}).markResolved
	_ = (&RefundService{}).revokeBenefits
	fmt.Println("refund_quickstart compiled successfully")
}
