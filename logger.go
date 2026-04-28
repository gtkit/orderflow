package orderflow

import "context"

// Logger 是 orderflow 的日志注入接口。
//
// # 设计原则
//
// 核心包**零外部日志依赖**——不导入 `log/slog`、`go.uber.org/zap`、`github.com/gtkit/logger`
// 任何具体日志框架，仅暴露此接口。业务方负责把自己的日志框架包装成 Logger 实现并注入。
//
// 接口刻意保持最小（4 个 level 方法 + 结构化 Field 列表 + ctx），让任何主流框架都能
// 一层薄包装实现。未注入时使用内置 nopLogger，所有日志被丢弃——库不假设业务方一定要日志。
//
// # 推荐实现：包装 github.com/gtkit/logger
//
//	type gtkitLogger struct{}
//
//	func (gtkitLogger) Debug(ctx context.Context, msg string, fs ...orderflow.Field) {
//	    logger.DebugCtx(ctx, msg, toZapFields(fs)...)
//	}
//	func (gtkitLogger) Info(ctx context.Context, msg string, fs ...orderflow.Field)  {
//	    logger.InfoCtx(ctx, msg, toZapFields(fs)...)
//	}
//	func (gtkitLogger) Warn(ctx context.Context, msg string, fs ...orderflow.Field)  {
//	    logger.WarnCtx(ctx, msg, toZapFields(fs)...)
//	}
//	func (gtkitLogger) Error(ctx context.Context, msg string, fs ...orderflow.Field) {
//	    logger.ErrorCtx(ctx, msg, toZapFields(fs)...)
//	}
//
//	func toZapFields(fs []orderflow.Field) []zap.Field {
//	    out := make([]zap.Field, len(fs))
//	    for i, f := range fs { out[i] = zap.Any(f.Key, f.Value) }
//	    return out
//	}
//
// # 实现契约
//
//   - 必须并发安全（Engine 在多 goroutine 内调用）
//   - 禁止 panic（与 Observer 同等约束；Engine 不对 Logger panic 加 recover）
//   - 必须非阻塞或带短超时——日志写入是热路径附属能力，慢实现会拖慢业务
//   - 对 ctx 取消应当尊重（典型如 ctx.Done 时短路），但不强制
type Logger interface {
	Debug(ctx context.Context, msg string, fields ...Field)
	Info(ctx context.Context, msg string, fields ...Field)
	Warn(ctx context.Context, msg string, fields ...Field)
	Error(ctx context.Context, msg string, fields ...Field)
}

// Field 是日志的结构化键值对。Value 可以是任意类型，由 Logger 实现负责序列化。
type Field struct {
	Key   string
	Value any
}

// String 构造字符串字段。
func String(key, value string) Field { return Field{Key: key, Value: value} }

// Int 构造 int 字段。
func Int(key string, value int) Field { return Field{Key: key, Value: value} }

// Int64 构造 int64 字段。
func Int64(key string, value int64) Field { return Field{Key: key, Value: value} }

// Uint64 构造 uint64 字段。
func Uint64(key string, value uint64) Field { return Field{Key: key, Value: value} }

// Any 构造任意类型字段，由 Logger 实现负责类型反射或序列化。
func Any(key string, value any) Field { return Field{Key: key, Value: value} }

// Err 构造 "error" 字段。约定字段名固定为 "error"，便于业务侧统一过滤。
func Err(err error) Field { return Field{Key: "error", Value: err} }

// nopLogger 是 Logger 的零开销默认实现，所有方法什么都不做。
// Engine 在 Config.Logger 未注入时使用此实现，让"不关心日志"的接入方零成本运行。
type nopLogger struct{}

func (nopLogger) Debug(context.Context, string, ...Field) {}
func (nopLogger) Info(context.Context, string, ...Field)  {}
func (nopLogger) Warn(context.Context, string, ...Field)  {}
func (nopLogger) Error(context.Context, string, ...Field) {}