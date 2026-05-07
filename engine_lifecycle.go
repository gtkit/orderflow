package orderflow

import (
	"context"
	"fmt"
	"net"
	"time"
)

// Create 入参的字段上限。取值贴合底层 DB 列与支付网关实际限制：
//   - product_title varchar(255)，微信 Subject 限 128 字节；
//   - client_ip varbinary(16) 存 IPv4/IPv6 二进制，长度上限由 net.ParseIP 保证。
const (
	maxCreateProductTitleLen = 128
)

// Create 创建订单并请求支付。
//
// 典型流程：
//  1. 查找同用户 + 同商品的 Pending 订单；
//     - 若存在且 IsReusable -> 直接复用，重新请求支付参数；
//     - 若存在但不可复用 -> 关闭旧单再建新单（期间若支付抢跑，返回当前订单）；
//  2. 生成订单号 / token，落库；
//  3. 入延时队列（用于支付超时自动关闭）；失败则自我保护式关闭；
//  4. 写 Pending 状态缓存 + 触发 OnCreated 钩子；
//  5. 调用支付网关 UnifiedOrder 获取客户端拉起参数。
//
// 错误返回：
//   - 入参非法（UserID / Product.ID 为 0、Title 越界、PayMethod 缺失、Price ≤ 0、
//     ClientIP 格式错）-> ErrInvalidConfig
//   - Store / Gateway / DelayQueue 报错 -> 原样包装
//
// 关于 Product.Price 守护：库的核心语义是"用户付钱给订单"，PayAmount=0 在 UnifiedOrder /
// notify.TotalAmount / CASConfirmPaid(expectedAmount) 链路中无意义；底层 paymgr SDK 的
// OrderRequest.Validate 也强制 total_amount > 0。orderflow 在 Create 入口提前拒绝，
// 避免无效副作用（DB 写入 / 延时队列入队 / 缓存写入）+ 错误归属混乱。0 元订单（赠品 /
// 试用 / 会员体验）应由业务方在调 Engine.Create 之前 short-circuit，直接走履约路径。
func (e *Engine[O]) Create(ctx context.Context, req CreateRequest) (result *CreateResult[O], err error) {
	start := time.Now()
	defer func() {
		e.observer.Duration(ctx, OpCreate, time.Since(start), err)
	}()

	if req.UserID == 0 {
		return nil, fmt.Errorf("%w: UserID must not be zero", ErrInvalidConfig)
	}
	if req.Product.ID == 0 {
		return nil, fmt.Errorf("%w: Product.ID must not be zero", ErrInvalidConfig)
	}
	if n := len(req.Product.Title); n == 0 || n > maxCreateProductTitleLen {
		return nil, fmt.Errorf("%w: Product.Title length %d not in [1, %d]", ErrInvalidConfig, n, maxCreateProductTitleLen)
	}
	if req.PayMethod == 0 {
		return nil, fmt.Errorf("%w: PayMethod must be specified", ErrInvalidConfig)
	}
	if req.Product.Price <= 0 {
		return nil, fmt.Errorf("%w: Product.Price must be > 0, got %d", ErrInvalidConfig, req.Product.Price)
	}
	if req.ClientIP != "" && net.ParseIP(req.ClientIP) == nil {
		return nil, fmt.Errorf("%w: ClientIP %q is not a valid IP address", ErrInvalidConfig, req.ClientIP)
	}

	// 可选分布式锁：配置 Locker 后，同用户同商品的 Create 串行化。
	//
	// 锁的范围：仅覆盖 FindPending → CASClose（superseded）→ store.Create → Enqueue
	// 这段"产生新 Pending 行"的关键区段。**支付网关 RTT 必须在锁外**——否则网关偶发
	// 慢于 createLockTTL 时锁会过期，并发 Create 拿到锁后再次创建 Pending，破坏
	// "一用户一商品一 Pending"不变量。
	var unlock func()
	if e.locker != nil {
		lockKey := fmt.Sprintf("orderflow:create:%d:%d", req.UserID, req.Product.ID)
		got, ok, lockErr := e.locker.TryLock(ctx, lockKey, e.createLockTTL)
		if lockErr != nil {
			err = fmt.Errorf("orderflow: acquire create lock: %w", lockErr)
			return nil, err
		}
		if !ok {
			// got 在 ok=false 时按契约是 no-op，但仍调用一次保证幂等。
			got()
			e.observer.Event(ctx, EventAnomaly, "", map[string]any{
				"kind":       "concurrent_create_rejected",
				"user_id":    req.UserID,
				"product_id": req.Product.ID,
			})
			err = ErrConcurrentCreate
			return nil, err
		}
		unlock = got
		// defer 兜底：异常路径（panic / 早期 return）下确保锁释放。
		// 主流程会在网关下单**之前**显式 unlock，正常路径下这里只是 no-op。
		defer func() {
			if unlock != nil {
				unlock()
			}
		}()
	}

	existing, found, err := e.store.FindPendingByUserAndProduct(ctx, req.UserID, req.Product.ID)
	if err != nil {
		return nil, fmt.Errorf("orderflow: find pending order: %w", err)
	}
	if found {
		if e.isReusableOf(existing, req) {
			// 复用路径：本地不需要再持锁，提前释放后调用网关。
			if unlock != nil {
				unlock()
				unlock = nil
			}
			e.observer.Event(ctx, EventOrderReused, existing.OrderNo(), nil)
			return e.requestPayment(ctx, existing, true)
		}
		current, hasCurrent, supErr := e.closeSuperseded(ctx, existing, req.Product.ID)
		if supErr != nil {
			err = fmt.Errorf("orderflow: close superseded: %w", supErr)
			return nil, err
		}
		if hasCurrent {
			// 抢跑路径：旧单已被网关确认支付，返回旧单让客户端跳详情页。
			// 注意 current 的金额是**旧 product** 的——客户端 UI 应基于
			// CreateResult.Reused=true 引导用户而非展示新 product 的支付页。
			if unlock != nil {
				unlock()
				unlock = nil
			}
			return &CreateResult[O]{Order: current, Reused: true}, nil
		}
	}

	orderNo := e.generateOrderNo(req.UserID)
	orderToken := e.generateOrderToken(orderNo, req.UserID, req.Product.ID)
	expireAt := time.Now().Add(e.orderExpire)

	spec := OrderSpec{
		OrderNo:       orderNo,
		OrderToken:    orderToken,
		UserID:        req.UserID,
		Status:        StatusPending,
		ProductID:     req.Product.ID,
		ProductType:   req.Product.Type,
		ProductTitle:  req.Product.Title,
		OriginalPrice: req.Product.Price,
		DiscountPrice: 0,
		PayAmount:     req.Product.Price,
		PayMethod:     req.PayMethod,
		ChannelID:     req.ChannelID,
		ExpireAt:      expireAt,
		ClientIP:      req.ClientIP,
		Extra:         req.Product.Extra,
	}

	order, createErr := e.store.Create(ctx, spec)
	if createErr != nil {
		err = fmt.Errorf("orderflow: create order: %w", createErr)
		return nil, err
	}
	e.appendLog(ctx, order, StatusPending, StatusPending, "system", "created")
	e.observer.Event(ctx, EventOrderCreated, order.OrderNo(), map[string]any{
		"user_id":    order.UserID(),
		"product_id": order.ProductID(),
	})

	if _, enqErr := e.delayQueue.Enqueue(ctx, orderNo, expireAt); enqErr != nil {
		// rollback 路径：订单从未对业务侧可见（OnCreated 还未调用），
		// 此处仅做内部清理，不触发任何业务钩子，避免事件序列不对称。
		e.rollbackPendingOnEnqueueFail(ctx, order, expireAt)
		if unlock != nil {
			unlock()
			unlock = nil
		}
		err = fmt.Errorf("orderflow: enqueue delay close: %w", enqErr)
		return nil, err
	}

	if setErr := e.cache.Set(ctx, orderToken, req.UserID, StatusPending, expireAt); setErr != nil {
		e.logger.Warn(ctx, "orderflow: set pending status cache failed",
			String("order_token", orderToken),
			Any("error", setErr),
		)
	}

	// OnCreated 之前订单虽然已落库，但还未"对业务可见"。OnCreated 调用之后，
	// 业务侧可能基于该事件做后续动作；因此 OnCreated 必须在 Enqueue 成功之后
	// 调用——保证 OnCreated 一旦触发，对应的 OnClosed/OnPaid 必然有机会触发，
	// 事件序列保持对称。
	if e.onCreated != nil {
		hookErr := e.safeHookE(ctx, "OnCreated", orderNo, func() error {
			return e.onCreated(ctx, order)
		})
		if hookErr != nil {
			e.logger.Warn(ctx, "orderflow: OnCreated hook returned error",
				String("order_no", orderNo),
				Any("error", hookErr),
			)
		}
	}

	// 网关下单**走锁外**：此刻 store.Create + Enqueue 已完成，"一用户一商品一 Pending"
	// 不变量已通过 DB 与延时队列固化，不再需要锁串行化。
	if unlock != nil {
		unlock()
		unlock = nil
	}

	result, err = e.requestPayment(ctx, order, false)
	return result, err
}

// rollbackPendingOnEnqueueFail 在延时队列入队失败时，把刚落库的 Pending 订单自我保护式关闭。
// 否则这单永远不会被关闭，占坑影响"一用户一商品一 Pending"的不变量。
//
// **不触发业务钩子的原因**：本路径在 OnCreated 调用之前发生——业务侧从未感知该订单
// 存在，触发 OnClosed 会让事件总线/审计日志看到一个"凭空冒出来的关闭事件"。
// 仅记录内部日志 + Observer 事件，让运维可观测但不污染业务事件流。
//
// 失败路径说明：如果 CAS 也失败（DB 短时不可用），此订单暂时成为"孤儿 Pending"——
// 不在延时队列、状态仍是 Pending。兜底机制：`CloseFallback` 会扫描 DB 中过期 Pending
// 并关掉它（届时走标准 Close 路径触发 OnClosed，事件序列保持完整）。
func (e *Engine[O]) rollbackPendingOnEnqueueFail(ctx context.Context, order O, expireAt time.Time) {
	affected, err := e.store.CASClose(ctx, order.OrderNo())
	if err != nil {
		e.logger.Error(ctx, "orderflow: rollback CAS close failed, order will be reaped by CloseFallback scanner",
			String("order_no", order.OrderNo()),
			Any("error", err),
		)
		return
	}
	if affected == 0 {
		e.logger.Warn(ctx, "orderflow: rollback CAS close missed (state changed concurrently)",
			String("order_no", order.OrderNo()),
		)
		return
	}
	e.publishStatus(ctx, order.OrderToken(), order.UserID(), StatusClosed, expireAt)
	e.appendLog(ctx, order, StatusPending, StatusClosed, "system", "closed: delay queue enqueue failed")
	// Observer 事件保留（运维可见），但不调用 OnClosed 钩子。
	e.observer.Event(ctx, EventOrderClosed, order.OrderNo(), map[string]any{
		"reason": string(ClosedReasonEnqueueFail),
	})
}

// closeSuperseded 在用户发起新订单时关闭旧 Pending 单。
//
// 返回值语义：
//   - err != nil：网关关闭失败（仅 SupersededStrict 模式）或存储错误，调用方应中断；
//   - hasCurrent == true：CAS 抢跑失败（典型：支付回调先一步把订单 Paid），调用方应把 current 作为结果返回给客户端；
//   - hasCurrent == false：旧单已成功关闭，调用方可以继续创建新单。
//
// 网关失败处理：受 Config.CloseSupersededPolicy 控制。
//   - SupersededStrict（默认）：直接返回错误，Create 失败。
//   - SupersededDegraded：记 ALERT 日志，继续走本地 CAS Close + Create；
//     网关侧的旧订单清理由 CloseFallback 周期扫描兜底。
func (e *Engine[O]) closeSuperseded(ctx context.Context, existing O, newProductID uint64) (current O, hasCurrent bool, err error) {
	var zero O
	if afterErr := e.afterClose(ctx, existing); afterErr != nil {
		e.appendLog(ctx, existing, StatusPending, StatusPending, "system",
			"gateway close failed during replacement: "+afterErr.Error())
		if e.closeSupersededPolicy != SupersededDegraded {
			return zero, false, afterErr
		}
		// 降级：记 ALERT 让运维感知；继续走本地 CAS Close。
		// 网关侧的"旧单未关"由 CloseFallback 后续扫描重试兜底。
		e.logger.Error(ctx, "orderflow: ALERT close superseded gateway failed, degraded to local close",
			String("order_no", existing.OrderNo()),
			Any("error", afterErr),
		)
	}

	affected, casErr := e.store.CASClose(ctx, existing.OrderNo())
	if casErr != nil {
		return zero, false, fmt.Errorf("cas close superseded: %w", casErr)
	}
	if affected == 0 {
		refreshed, found, qErr := e.store.GetByNo(ctx, existing.OrderNo())
		if qErr != nil {
			return zero, false, fmt.Errorf("recheck superseded after race: %w", qErr)
		}
		if !found {
			return zero, false, nil
		}
		// Closed / Cancelled 意味着本次关闭已生效（或等效），可继续创建新单。
		if refreshed.Status() == StatusClosed || refreshed.Status() == StatusCancelled {
			return zero, false, nil
		}
		if refreshed.Status() == StatusPaid {
			e.appendLog(ctx, refreshed, StatusPending, StatusPaid, "system",
				"payment won race during replacement close")
		}
		return refreshed, true, nil
	}

	_ = e.delayQueue.Remove(ctx, existing.OrderNo())
	e.publishStatus(ctx, existing.OrderToken(), existing.UserID(), StatusClosed, existing.ExpireAt())
	e.appendLog(ctx, existing, StatusPending, StatusClosed, "system",
		fmt.Sprintf("superseded by new product %d", newProductID))

	e.observer.Event(ctx, EventOrderSuperseded, existing.OrderNo(), map[string]any{
		"new_product_id": newProductID,
	})
	e.observer.Event(ctx, EventOrderClosed, existing.OrderNo(), map[string]any{
		"reason": string(ClosedReasonSuperseded),
	})
	if e.onSuperseded != nil {
		e.safeHook(ctx, "OnSuperseded", existing.OrderNo(), func() {
			e.onSuperseded(ctx, existing, newProductID)
		})
	}
	if e.onClosed != nil {
		e.safeHook(ctx, "OnClosed", existing.OrderNo(), func() {
			e.onClosed(ctx, existing, ClosedReasonSuperseded)
		})
	}
	return zero, false, nil
}

// requestPayment 调用支付网关下单，返回客户端拉起支付所需参数。
func (e *Engine[O]) requestPayment(ctx context.Context, order O, reused bool) (*CreateResult[O], error) {
	channel := e.resolveChannelOf(order.PayMethod())
	notifyURL := e.buildNotifyURLOf(channel)

	resp, err := e.gateway.UnifiedOrder(ctx, channel, UnifiedOrderRequest{
		OutTradeNo:  order.OrderNo(),
		TotalAmount: order.PayAmount(),
		Subject:     order.ProductTitle(),
		NotifyURL:   notifyURL,
		ExpireAt:    order.ExpireAt(),
		Metadata: map[string]string{
			"order_token": order.OrderToken(),
		},
	})
	if err != nil {
		e.appendLog(ctx, order, order.Status(), order.Status(), "system",
			"payment request failed: "+err.Error())
		return nil, fmt.Errorf("orderflow: gateway unified order: %w", err)
	}

	return &CreateResult[O]{
		Order:         order,
		PaymentParams: resp.AppParams,
		Reused:        reused,
	}, nil
}
