package orderflow

import (
	"context"
	"testing"
)

// Subscribe 只是透传到 StatusStream driver——最小测试保证不 panic + 能拿到 Events() chan。
// 真实语义（事件顺序、Close 幂等）由 rediscache driver 的测试保证。

func TestSubscribe_DelegatesToStream(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	sub, err := env.engine.Subscribe(ctx, "TOK-SUB")
	mustNotErr(t, err, "Subscribe")
	if sub == nil {
		t.Fatal("Subscribe returned nil")
	}
	// Events() 返回只读 chan 且 Close 幂等
	ch := sub.Events()
	if ch == nil {
		t.Error("Events chan is nil")
	}
	mustNotErr(t, sub.Close(), "Close")
	mustNotErr(t, sub.Close(), "Close idempotent")
}
