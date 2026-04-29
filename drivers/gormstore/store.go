package gormstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderflow"
	"gorm.io/gorm"
)

// Config 配置 gormstore.Store。
//
// 必填：
//   - DB：*gorm.DB
//   - OrderTable / BillTable / LogTable：三张表的表名
//   - Wrap：把 *M 包装成 orderflow.OrderSnapshot 的 O
//   - BuildModel：按 OrderSpec 构造 *M 交给 GORM Create
//
// 可选：
//   - ColumnMap：订单表列名覆盖（零值字段走默认值）
//   - BillWriter：自定义账单持久化实现；零值使用内置默认实现（按 OrderBill struct 写入 BillTable），
//     业务方账单表结构与内置模型不一致时实现此接口替换。
//   - LogStore：自定义流水持久化实现；零值使用内置默认实现（按 OrderLog struct 读写 LogTable）。
//   - FinalizeExtra：FinalizePaidOrder 事务内的业务扩展钩子，**仅允许同事务内的
//     DB 操作**——禁止在此发起 RPC、HTTP、消息队列等外部 IO，否则会让该订单的
//     行锁持有时间膨胀到外部 RTT 量级，引发热点行锁堆积。外部副作用应放在
//     OnDelivered 钩子（旁路、不阻断）或独立的事件总线消费链路。
//     bill 参数为已合并 channel_id 回查结果的 BillSpec 副本，与同次调用传给
//     BillWriter.Write 的实参完全一致。
//     升级提示：v1.3.x 的旧签名收 *OrderBill，当前改为 orderflow.BillSpec
//     中性载荷——字段名一一对应，仅 bill.ID（ORM 主键）需改为 bill.OrderNo
//     做事务内关联。完整迁移示例与字段映射表见 README "FinalizeExtra 签名升级" 小节。
//   - PaidUndeliveredRetryGrace：FindPaidUndelivered 的 paid_at 时间窗口下界。
//     该值用于过滤"刚 Paid 不久、正在被正常 OnPaid 路径处理"的订单，避免
//     DeliveryFallback 与正常路径反复抢锁导致 OnPaid 被无谓重入。零值使用默认
//     60s。业务对补偿延迟敏感时可调小，但不建议小于 10s。
type Config[O orderflow.OrderSnapshot, M any] struct {
	DB *gorm.DB

	OrderTable string
	BillTable  string
	LogTable   string

	Wrap       func(*M) O
	BuildModel func(spec orderflow.OrderSpec) *M

	ColumnMap ColumnMap

	BillWriter BillWriter
	LogStore   LogStore

	FinalizeExtra func(tx *gorm.DB, order O, bill orderflow.BillSpec) error

	PaidUndeliveredRetryGrace time.Duration
}

func (c Config[O, M]) validate() error {
	switch {
	case c.DB == nil:
		return fmt.Errorf("gormstore: DB must not be nil")
	case c.OrderTable == "":
		return fmt.Errorf("gormstore: OrderTable must not be empty")
	case c.BillTable == "":
		return fmt.Errorf("gormstore: BillTable must not be empty")
	case c.LogTable == "":
		return fmt.Errorf("gormstore: LogTable must not be empty")
	case c.Wrap == nil:
		return fmt.Errorf("gormstore: Wrap must not be nil")
	case c.BuildModel == nil:
		return fmt.Errorf("gormstore: BuildModel must not be nil")
	}
	// 表名与列名走同样的 SQL 标识符白名单校验（防御外部配置注入：
	// 如果表名来自 yaml / consul / env，恶意配置可通过表名注入 SQL）。
	tables := []struct {
		name, val string
	}{
		{"OrderTable", c.OrderTable},
		{"BillTable", c.BillTable},
		{"LogTable", c.LogTable},
	}
	for _, t := range tables {
		if !SQLIdentifierPattern.MatchString(t.val) {
			return fmt.Errorf("gormstore: %s %q is not a valid SQL identifier (must match [a-zA-Z_][a-zA-Z0-9_]*)", t.name, t.val)
		}
	}
	if err := c.ColumnMap.validate(); err != nil {
		return err
	}
	return nil
}

// defaultPaidUndeliveredRetryGrace 是 FindPaidUndelivered 的默认时间窗口下界。
// 给"刚 Paid"的订单留 60s 走正常 OnPaid 路径，避免 fallback worker 立即抢入。
const defaultPaidUndeliveredRetryGrace = 60 * time.Second

// Store 基于 GORM 的 orderflow.Store[O] 实现。
type Store[O orderflow.OrderSnapshot, M any] struct {
	db                        *gorm.DB
	orderTable                string
	cols                      ColumnMap
	wrap                      func(*M) O
	buildModel                func(orderflow.OrderSpec) *M
	billWriter                BillWriter
	logStore                  LogStore
	finalizeExtra             func(tx *gorm.DB, order O, bill orderflow.BillSpec) error
	paidUndeliveredRetryGrace time.Duration
}

// New 构造 Store。参数非法时返回错误。
//
// Config.BillWriter / Config.LogStore 为零值时分别注入内置默认实现，行为与历史版本
// 等价。
func New[O orderflow.OrderSnapshot, M any](cfg Config[O, M]) (*Store[O, M], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	grace := cfg.PaidUndeliveredRetryGrace
	if grace <= 0 {
		grace = defaultPaidUndeliveredRetryGrace
	}
	billWriter := cfg.BillWriter
	if billWriter == nil {
		billWriter = newDefaultBillWriter(cfg.BillTable)
	}
	logStore := cfg.LogStore
	if logStore == nil {
		logStore = newDefaultLogStore(cfg.LogTable)
	}
	return &Store[O, M]{
		db:                        cfg.DB,
		orderTable:                cfg.OrderTable,
		cols:                      cfg.ColumnMap.withDefaults(),
		wrap:                      cfg.Wrap,
		buildModel:                cfg.BuildModel,
		billWriter:                billWriter,
		logStore:                  logStore,
		finalizeExtra:             cfg.FinalizeExtra,
		paidUndeliveredRetryGrace: grace,
	}, nil
}

// ----- 读路径 -----

func (s *Store[O, M]) GetByNo(ctx context.Context, orderNo string, fields ...string) (O, bool, error) {
	return s.getOne(ctx, s.cols.OrderNo, orderNo, fields)
}

func (s *Store[O, M]) GetByToken(ctx context.Context, orderToken string) (O, bool, error) {
	return s.getOne(ctx, s.cols.OrderToken, orderToken, nil)
}

func (s *Store[O, M]) getOne(ctx context.Context, column string, value any, fields []string) (O, bool, error) {
	var zero O
	m := new(M)
	q := s.db.WithContext(ctx).Table(s.orderTable).Where(column+" = ?", value)
	if len(fields) > 0 {
		q = q.Select(fields)
	}
	err := q.First(m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("gormstore: get by %s: %w", column, err)
	}
	return s.wrap(m), true, nil
}

func (s *Store[O, M]) ListByUser(ctx context.Context, userID int64, fields ...string) ([]O, error) {
	var ms []*M
	q := s.db.WithContext(ctx).Table(s.orderTable).Where(s.cols.UserID+" = ?", userID)
	if len(fields) > 0 {
		q = q.Select(fields)
	}
	if err := q.Find(&ms).Error; err != nil {
		return nil, fmt.Errorf("gormstore: list by user: %w", err)
	}
	out := make([]O, len(ms))
	for i, m := range ms {
		out[i] = s.wrap(m)
	}
	return out, nil
}

// FindPendingByUserAndProduct 返回该用户+该商品最新的一条 Pending 订单。
//
// 显式按 updated_at DESC 排序的原因：理论上"一用户一商品一 Pending"是不变量，
// 但若历史上漏防御产生过多条同 (user, product, pending)，GORM 默认按主键排序
// 会让"复用单"的选择依赖 DB 内部状态。这里固定取最新一条作为复用对象，旧的让
// CloseFallback 在 ExpireAt 后回收。
func (s *Store[O, M]) FindPendingByUserAndProduct(ctx context.Context, userID int64, productID uint64) (O, bool, error) {
	var zero O
	m := new(M)
	err := s.db.WithContext(ctx).Table(s.orderTable).
		Where(s.cols.UserID+" = ? AND "+s.cols.ProductID+" = ? AND "+s.cols.Status+" = ?",
			userID, productID, orderflow.StatusPending).
		Order(s.cols.UpdatedAt + " DESC").
		First(m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("gormstore: find pending by user+product: %w", err)
	}
	return s.wrap(m), true, nil
}

// FindExpiredPending 返回已过期且仍为 Pending 的订单号，供 CloseFallback 兜底。
//
// 按 expire_at ASC 排序：多 worker 副本看到一致的扫描优先级（先关闭最早过期的），
// 减少多实例同时抢同一批订单导致的 CAS 抢锁浪费。Close 本身幂等不会出错，但行锁
// 竞争会拖慢正常订单处理。
func (s *Store[O, M]) FindExpiredPending(ctx context.Context, limit int) ([]string, error) {
	return s.findOrderNos(ctx,
		s.cols.Status+" = ? AND "+s.cols.ExpireAt+" < ?",
		[]any{orderflow.StatusPending, time.Now()},
		limit,
		s.cols.ExpireAt+" ASC",
	)
}

// FindPaidUndelivered 返回"已 Paid 但尚未 Delivered"的订单号，供 DeliveryFallback 兜底。
//
// 时间窗口：仅返回 paid_at < NOW - PaidUndeliveredRetryGrace 的订单。
// 这层窗口给"刚 Paid 的订单"留出走正常 OnPaid 路径的时间，避免 fallback worker
// 与正常 finalizeDelivery 抢行锁，反复触发 OnPaid 钩子（业务侧虽然能靠幂等收敛，
// 但避免不必要的重入仍是更好的设计）。
func (s *Store[O, M]) FindPaidUndelivered(ctx context.Context, limit int) ([]string, error) {
	cutoff := time.Now().Add(-s.paidUndeliveredRetryGrace)
	return s.findOrderNos(ctx,
		s.cols.Status+" = ? AND "+s.cols.PaidAt+" < ?",
		[]any{orderflow.StatusPaid, cutoff},
		limit,
		s.cols.PaidAt+" ASC",
	)
}

// findOrderNos 公共扫描路径。orderBy 必须是已校验的列名 + ASC/DESC，调用方控制。
func (s *Store[O, M]) findOrderNos(ctx context.Context, where string, args []any, limit int, orderBy string) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	q := s.db.WithContext(ctx).Table(s.orderTable).
		Where(where, args...).
		Limit(limit)
	if orderBy != "" {
		q = q.Order(orderBy)
	}
	var orderNos []string
	if err := q.Pluck(s.cols.OrderNo, &orderNos).Error; err != nil {
		return nil, fmt.Errorf("gormstore: find order nos: %w", err)
	}
	return orderNos, nil
}
