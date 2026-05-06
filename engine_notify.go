package orderflow

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// HandleNotify 处理支付网关的回调请求（典型场景：微信 / 支付宝异步通知）。
//
// 流程概要：
//  1. 调用 Gateway.ParseNotify 解析并验签；
//  2. 非 Paid 状态的通知直接忽略（退款、关闭等在独立管道处理）；
//  3. 根据订单当前状态分派：
//     - Delivered/Completed：幂等 skip；
//     - Paid：重入 finalize（OnPaid + FinalizePaidOrder）；
//     - Closed：走"关闭后又支付成功"的恢复路径；
//     - Pending：金额校验 + CAS 推进 Paid + finalize；
//  4. 中间步骤失败不会返回错误给网关，避免重试风暴；由 fallback scanner 兜底。
//
// 返回错误仅限于致命情况（解析失败、CAS 系统错误等），业务异常走 OnAnomaly 钩子告警。
func (e *Engine[O]) HandleNotify(ctx context.Context, ch Channel, req *http.Request) (err error) {
	start := time.Now()
	defer func() {
		e.observer.Duration(ctx, OpHandleNotify, time.Since(start), err)
	}()

	var notify NotifyResult
	notify, err = e.gateway.ParseNotify(ctx, ch, req)
	if err != nil {
		return fmt.Errorf("orderflow: parse notify: %w", err)
	}

	// 防御性长度校验：即使 ParseNotify 已验签，底层协议未必限制字段长度。
	// 超长 TradeNo 会触发 DB 列截断，超长 OutTradeNo 会让 GetByNo 扫全表——双重风险。
	if !validNotifyLength(notify) {
		e.logger.Warn(ctx, "orderflow: reject notify with oversized fields",
			Int("out_trade_no_len", len(notify.OutTradeNo)),
			Int("transaction_id_len", len(notify.TransactionID)),
		)
		return nil
	}

	if notify.TradeStatus != TradeStatusPaid {
		e.logger.Info(ctx, "orderflow: notify not paid, skip",
			String("trade_status", string(notify.TradeStatus)),
			String("out_trade_no", notify.OutTradeNo),
		)
		return nil
	}
	e.normalizeNotifyPaidAt(&notify)

	order, found, err := e.store.GetByNo(ctx, notify.OutTradeNo)
	if err != nil {
		return fmt.Errorf("orderflow: query order for notify: %w", err)
	}
	if !found {
		e.logger.Error(ctx, "orderflow: ALERT: order not found for paid notify",
			String("out_trade_no", notify.OutTradeNo),
		)
		return nil
	}

	switch order.Status() {
	case StatusDelivered, StatusCompleted:
		e.logger.Info(ctx, "orderflow: order already delivered, idempotent skip",
			String("order_no", order.OrderNo()),
			String("status", order.Status().String()),
		)
		return nil
	case StatusPaid:
		return e.retryFinalizeForPaid(ctx, order, notify)
	case StatusClosed:
		return e.handleClosedPaidNotify(ctx, order, notify)
	case StatusPending:
		// 继续下方的主流程
	default:
		e.recordAnomaly(ctx, order, AnomalyUnexpectedStatus,
			fmt.Sprintf("non-pending order received paid notify: status=%s", order.Status()))
		return nil
	}

	if notify.TotalAmount != order.PayAmount() {
		e.recordAnomaly(ctx, order, AnomalyAmountMismatch,
			fmt.Sprintf("notify=%d order=%d", notify.TotalAmount, order.PayAmount()))
		return nil
	}

	affected, err := e.store.CASConfirmPaid(ctx, notify.OutTradeNo, notify.TransactionID, notify.PaidAt, order.PayAmount())
	if err != nil {
		return fmt.Errorf("orderflow: cas confirm paid: %w", err)
	}
	if affected == 0 {
		return e.recheckAfterCASFailed(ctx, notify)
	}

	e.logger.Info(ctx, "orderflow: order paid successfully",
		String("order_no", order.OrderNo()),
		Int64("amount", notify.TotalAmount),
		String("trade_no", notify.TransactionID),
	)
	e.appendLog(ctx, order, StatusPending, StatusPaid, "system",
		"paid: trade_no="+notify.TransactionID)
	e.observer.Event(ctx, EventOrderPaid, order.OrderNo(), map[string]any{
		"trade_no": notify.TransactionID,
		"amount":   notify.TotalAmount,
	})

	if err := e.delayQueue.Remove(ctx, notify.OutTradeNo); err != nil {
		// 残留订单号会被 CloseWorker 二次拉取，对 Paid 订单走幂等 skip 路径不影响正确性，
		// 但会污染 close 路径的事件 / 日志计数；通过 anomaly 让监控感知 Queue 可用性。
		e.recordAnomaly(ctx, order, AnomalyDelayQueueCleanupFailed,
			"remove delay close failed: "+err.Error())
	}
	e.publishStatus(ctx, order.OrderToken(), order.UserID(), StatusPaid, order.ExpireAt())

	// CAS 已经把 trade_no / paid_at 写进去；finalize 需要最新快照。
	refreshed := e.reloadOrder(ctx, order)

	if err := e.finalizeDelivery(ctx, refreshed, notify); err != nil {
		e.logger.Error(ctx, "orderflow: finalize failed, ack gateway and rely on fallback",
			String("order_no", refreshed.OrderNo()),
			Any("error", err),
		)
	}
	return nil
}

// retryFinalizeForPaid 处理"订单已是 Paid 状态时的重复通知"。
// 通常是支付网关重试推送、或 OnPaid 首次失败后我们主动让网关重发。
func (e *Engine[O]) retryFinalizeForPaid(ctx context.Context, order O, notify NotifyResult) error {
	if notify.TotalAmount != order.PayAmount() {
		e.recordAnomaly(ctx, order, AnomalyAmountMismatch,
			fmt.Sprintf("paid retry amount mismatch: notify=%d order=%d", notify.TotalAmount, order.PayAmount()))
		return nil
	}
	if order.TradeNo() != "" && order.TradeNo() != notify.TransactionID {
		e.recordAnomaly(ctx, order, AnomalyTradeNoMismatch,
			fmt.Sprintf("stored=%s notify=%s", order.TradeNo(), notify.TransactionID))
		return nil
	}
	e.logger.Info(ctx, "orderflow: order already paid, retry finalize",
		String("order_no", order.OrderNo()),
	)
	if err := e.finalizeDelivery(ctx, order, notify); err != nil {
		e.logger.Error(ctx, "orderflow: finalize retry failed, rely on fallback",
			String("order_no", order.OrderNo()),
			Any("error", err),
		)
	}
	return nil
}

// recheckAfterCASFailed 在 CASConfirmPaid 返回 affected=0 时复查订单当前状态，
// 根据结果走对应的分派逻辑。
func (e *Engine[O]) recheckAfterCASFailed(ctx context.Context, notify NotifyResult) error {
	current, found, err := e.store.GetByNo(ctx, notify.OutTradeNo)
	if err != nil {
		return fmt.Errorf("orderflow: recheck order: %w", err)
	}
	if !found {
		e.logger.Error(ctx, "orderflow: ALERT: order disappeared during notify recheck",
			String("out_trade_no", notify.OutTradeNo),
		)
		return nil
	}
	switch current.Status() {
	case StatusPaid:
		return e.retryFinalizeForPaid(ctx, current, notify)
	case StatusDelivered, StatusCompleted:
		e.logger.Info(ctx, "orderflow: order already delivered (duplicate notify)",
			String("order_no", current.OrderNo()),
		)
		return nil
	case StatusClosed:
		return e.handleClosedPaidNotify(ctx, current, notify)
	default:
		e.recordAnomaly(ctx, current, AnomalyUnexpectedStatus,
			fmt.Sprintf("CAS failed with unexpected status: %s", current.Status()))
		return nil
	}
}

// handleClosedPaidNotify 处理"订单已关闭但支付网关确认已支付"的竞态。
// 策略：向网关查询确认真实状态 → CASReopenPaid → finalize。
func (e *Engine[O]) handleClosedPaidNotify(ctx context.Context, order O, notify NotifyResult) error {
	channel := e.resolveChannelOf(order.PayMethod())

	query, err := retryN(ctx, 3, 100*time.Millisecond, func() (QueryResult, error) {
		return e.gateway.QueryOrder(ctx, channel, order.OrderNo())
	})
	if err != nil {
		e.recordAnomaly(ctx, order, AnomalyGatewayQueryFailed, err.Error())
		return nil
	}

	if query.TradeStatus != TradeStatusPaid {
		e.recordAnomaly(ctx, order, AnomalyPaidOnClosed,
			fmt.Sprintf("closed order notify but gateway status=%s, skip", query.TradeStatus))
		return nil
	}
	if query.TotalAmount != order.PayAmount() {
		e.recordAnomaly(ctx, order, AnomalyAmountMismatch,
			fmt.Sprintf("closed reopen mismatch: gateway=%d order=%d", query.TotalAmount, order.PayAmount()))
		return nil
	}

	affected, err := e.store.CASReopenPaid(ctx, order.OrderNo(), query.TransactionID, query.PaidAt, order.PayAmount())
	if err != nil {
		e.recordAnomaly(ctx, order, AnomalyUnexpectedStatus, "cas reopen failed: "+err.Error())
		return nil
	}
	if affected == 0 {
		e.logger.Info(ctx, "orderflow: cas reopen affected 0, concurrent op won",
			String("order_no", order.OrderNo()),
		)
		return nil
	}

	e.logger.Info(ctx, "orderflow: closed order reopened to paid",
		String("order_no", order.OrderNo()),
		String("transaction_id", query.TransactionID),
	)
	e.appendLog(ctx, order, StatusClosed, StatusPaid, "system",
		"auto reopen: payment confirmed after close")
	e.observer.Event(ctx, EventOrderReopened, order.OrderNo(), map[string]any{
		"trade_no": query.TransactionID,
	})
	e.observer.Event(ctx, EventOrderPaid, order.OrderNo(), map[string]any{
		"trade_no": query.TransactionID,
		"amount":   query.TotalAmount,
	})

	e.publishStatus(ctx, order.OrderToken(), order.UserID(), StatusPaid, order.ExpireAt())

	confirmed := buildConfirmedNotify(notify, query, channel)
	e.normalizeNotifyPaidAt(&confirmed)

	refreshed := e.reloadOrder(ctx, order)

	if e.onReopened != nil {
		e.safeHook(ctx, "OnReopened", refreshed.OrderNo(), func() {
			e.onReopened(ctx, refreshed, confirmed)
		})
	}

	if err := e.finalizeDelivery(ctx, refreshed, confirmed); err != nil {
		e.logger.Error(ctx, "orderflow: finalize after reopen failed, rely on fallback",
			String("order_no", refreshed.OrderNo()),
			Any("error", err),
		)
	}
	return nil
}

// finalizeDelivery 完成履约：调用 OnPaid 钩子 + Store.FinalizePaidOrder + 推送 Delivered。
//
// 时序：
//  1. OnPaid 钩子（业务侧权益发放，必须幂等）
//  2. Store.FinalizePaidOrder（订单 -> Delivered，插账单）
//  3. 状态广播
//  4. OnDelivered 钩子（旁路观察）
//
// 任一步骤失败都会触发 AnomalyDeliveryFailed，由 fallback scanner 后续补偿。
func (e *Engine[O]) finalizeDelivery(ctx context.Context, order O, notify NotifyResult) error {
	if e.onPaid != nil {
		hookErr := e.safeHookE(ctx, "OnPaid", order.OrderNo(), func() error {
			return e.onPaid(ctx, order, notify)
		})
		if hookErr != nil {
			e.recordAnomaly(ctx, order, AnomalyDeliveryFailed, "OnPaid: "+hookErr.Error())
			return fmt.Errorf("orderflow: OnPaid: %w", hookErr)
		}
	}

	bill := BillSpec{
		UserID:         order.UserID(),
		OrderNo:        order.OrderNo(),
		TradeNo:        notify.TransactionID,
		ProductID:      order.ProductID(),
		ProductType:    order.ProductType(),
		ProductTitle:   order.ProductTitle(),
		OriginalPrice:  order.OriginalPrice(),
		DiscountAmount: order.OriginalPrice() - order.PayAmount(),
		PayAmount:      order.PayAmount(),
		PayMethod:      order.PayMethod(),
		PayChannel:     string(notify.Channel),
		PaidAt:         notify.PaidAt,
	}
	if err := e.store.FinalizePaidOrder(ctx, order, bill); err != nil {
		e.recordAnomaly(ctx, order, AnomalyDeliveryFailed, "finalize paid order: "+err.Error())
		return fmt.Errorf("orderflow: finalize paid order: %w", err)
	}

	e.publishStatus(ctx, order.OrderToken(), order.UserID(), StatusDelivered, order.ExpireAt())
	e.observer.Event(ctx, EventOrderDelivered, order.OrderNo(), map[string]any{
		"user_id": order.UserID(),
	})

	if e.onDelivered != nil {
		hookErr := e.safeHookE(ctx, "OnDelivered", order.OrderNo(), func() error {
			return e.onDelivered(ctx, order)
		})
		if hookErr != nil {
			e.logger.Warn(ctx, "orderflow: OnDelivered hook returned error",
				String("order_no", order.OrderNo()),
				Any("error", hookErr),
			)
		}
	}
	return nil
}

// normalizeNotifyPaidAt 把回调的支付时间归一化到 Engine 配置的时区。
// 更复杂的渠道特异处理（如支付宝返回本地时间需重解释）应由 gateway driver 在 ParseNotify 内部完成。
func (e *Engine[O]) normalizeNotifyPaidAt(notify *NotifyResult) {
	if notify == nil || notify.PaidAt.IsZero() {
		return
	}
	notify.PaidAt = notify.PaidAt.In(e.location)
}

// reloadOrder 尝试重新从 Store 读最新快照；失败则返回原 order 并发出 ALERT。
//
// 调用方（HandleNotify）在 CAS 成功后会调用 reloadOrder 取含 trade_no / paid_at 的新快照。
// 即使失败 finalize 仍能继续跑（bill.TradeNo 与 PaidAt 取自 notify，不依赖 order 快照），
// 但 OnPaid 钩子拿到的 order.Status() 可能仍是 CAS 前的旧值——所以这里把日志级别从
// Warn 提到 Error 并附 ALERT 关键字，同时发 EventAnomaly 让监控能感知。
func (e *Engine[O]) reloadOrder(ctx context.Context, order O) O {
	refreshed, found, err := e.store.GetByNo(ctx, order.OrderNo())
	if err != nil {
		e.logger.Error(ctx, "orderflow: ALERT: reload order failed, proceeding with stale snapshot",
			String("order_no", order.OrderNo()),
			Any("error", err),
		)
		e.observer.Event(ctx, EventAnomaly, order.OrderNo(), map[string]any{
			"kind":   "reload_failed",
			"reason": err.Error(),
		})
		return order
	}
	if !found {
		e.logger.Error(ctx, "orderflow: ALERT: order disappeared during reload",
			String("order_no", order.OrderNo()),
		)
		e.observer.Event(ctx, EventAnomaly, order.OrderNo(), map[string]any{
			"kind": "reload_disappeared",
		})
		return order
	}
	return refreshed
}

// validNotifyLength 校验 NotifyResult 的字符串字段未超过 DB 列上限。
// 数字来源于 gormstore 内置 Bill 模型（varchar(64)/(128)）和常见支付网关限制。
func validNotifyLength(n NotifyResult) bool {
	const (
		maxOutTradeNoLen    = 64
		maxTransactionIDLen = 128
	)
	if len(n.OutTradeNo) == 0 || len(n.OutTradeNo) > maxOutTradeNoLen {
		return false
	}
	if len(n.TransactionID) > maxTransactionIDLen {
		return false
	}
	return true
}

// buildConfirmedNotify 用网关查询结果覆盖回调字段，得到"以网关为准"的 NotifyResult。
//
// 覆盖策略：所有字段"非零值覆盖、零值保留"。
//
//   - 字符串字段非空才覆盖，避免网关返回空串污染；
//   - TotalAmount > 0 才覆盖，避免网关解析 bug 返回 0 把金额清零；
//   - PaidAt / TradeStatus / Channel 同理——网关驱动偶发异常不应让 NotifyResult 变成无效值。
//
// 调用方（handleClosedPaidNotify）已在进入此函数前校验 query.TradeStatus == Paid 且金额匹配，
// 但这层防御仍然有意义：避免未来新增的 caller 忘记上游守卫。
func buildConfirmedNotify(original NotifyResult, query QueryResult, channel Channel) NotifyResult {
	confirmed := original
	if query.OutTradeNo != "" {
		confirmed.OutTradeNo = query.OutTradeNo
	}
	if query.TransactionID != "" {
		confirmed.TransactionID = query.TransactionID
	}
	if query.TradeStatus != "" {
		confirmed.TradeStatus = query.TradeStatus
	}
	if query.TotalAmount > 0 {
		confirmed.TotalAmount = query.TotalAmount
	}
	if !query.PaidAt.IsZero() {
		confirmed.PaidAt = query.PaidAt
	}
	if query.Channel != "" {
		confirmed.Channel = query.Channel
	} else if confirmed.Channel == "" {
		confirmed.Channel = channel
	}
	return confirmed
}
