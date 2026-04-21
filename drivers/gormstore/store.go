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
//   - FinalizeExtra：FinalizePaidOrder 事务内的业务扩展钩子，用于在同一事务里激活 VIP / 发放积分等
type Config[O orderflow.OrderSnapshot, M any] struct {
	DB *gorm.DB

	OrderTable string
	BillTable  string
	LogTable   string

	Wrap       func(*M) O
	BuildModel func(spec orderflow.OrderSpec) *M

	ColumnMap ColumnMap

	FinalizeExtra func(tx *gorm.DB, order O, bill *OrderBill) error
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

// Store 基于 GORM 的 orderflow.Store[O] 实现。
type Store[O orderflow.OrderSnapshot, M any] struct {
	db            *gorm.DB
	orderTable    string
	billTable     string
	logTable      string
	cols          ColumnMap
	wrap          func(*M) O
	buildModel    func(orderflow.OrderSpec) *M
	finalizeExtra func(tx *gorm.DB, order O, bill *OrderBill) error
}

// New 构造 Store。参数非法时返回错误。
func New[O orderflow.OrderSnapshot, M any](cfg Config[O, M]) (*Store[O, M], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Store[O, M]{
		db:            cfg.DB,
		orderTable:    cfg.OrderTable,
		billTable:     cfg.BillTable,
		logTable:      cfg.LogTable,
		cols:          cfg.ColumnMap.withDefaults(),
		wrap:          cfg.Wrap,
		buildModel:    cfg.BuildModel,
		finalizeExtra: cfg.FinalizeExtra,
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

func (s *Store[O, M]) FindPendingByUserAndProduct(ctx context.Context, userID int64, productID uint64) (O, bool, error) {
	var zero O
	m := new(M)
	err := s.db.WithContext(ctx).Table(s.orderTable).
		Where(s.cols.UserID+" = ? AND "+s.cols.ProductID+" = ? AND "+s.cols.Status+" = ?",
			userID, productID, orderflow.StatusPending).
		First(m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("gormstore: find pending by user+product: %w", err)
	}
	return s.wrap(m), true, nil
}

func (s *Store[O, M]) FindExpiredPending(ctx context.Context, limit int) ([]string, error) {
	return s.findOrderNos(ctx,
		s.cols.Status+" = ? AND "+s.cols.ExpireAt+" < ?",
		[]any{orderflow.StatusPending, time.Now()},
		limit,
	)
}

func (s *Store[O, M]) FindPaidUndelivered(ctx context.Context, limit int) ([]string, error) {
	return s.findOrderNos(ctx,
		s.cols.Status+" = ?",
		[]any{orderflow.StatusPaid},
		limit,
	)
}

func (s *Store[O, M]) findOrderNos(ctx context.Context, where string, args []any, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	var orderNos []string
	err := s.db.WithContext(ctx).Table(s.orderTable).
		Where(where, args...).
		Limit(limit).
		Pluck(s.cols.OrderNo, &orderNos).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: find order nos: %w", err)
	}
	return orderNos, nil
}
