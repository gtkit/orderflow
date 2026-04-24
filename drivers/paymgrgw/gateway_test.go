package paymgrgw

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/gtkit/go-pay/paymgr"
	"github.com/gtkit/orderflow"
)

// 其他 Gateway 方法都是纯粹的类型转换 wrapper，测试价值等同于重抄一遍代码。
// IsIgnorableCloseError 是这一层唯一的非平凡逻辑：判断哪些网关错误可以静默吞下。

func TestIsIgnorableCloseError_NilIsNotIgnorable(t *testing.T) {
	g := New(paymgr.NewManager())
	if g.IsIgnorableCloseError(orderflow.Channel("wechat"), nil) {
		t.Error("nil error should not be ignorable")
	}
}

func TestIsIgnorableCloseError_SentinelErrorsAreIgnored(t *testing.T) {
	g := New(paymgr.NewManager())
	for _, err := range []error{paymgr.ErrOrderNotFound, paymgr.ErrOrderClosed} {
		if !g.IsIgnorableCloseError(orderflow.Channel("wechat"), err) {
			t.Errorf("expected %v to be ignorable", err)
		}
	}
	// 透过 fmt.Errorf wrap 后依然被 errors.Is 识别
	wrapped := fmt.Errorf("close failed: %w", paymgr.ErrOrderNotFound)
	if !g.IsIgnorableCloseError(orderflow.Channel("wechat"), wrapped) {
		t.Error("wrapped ErrOrderNotFound should be ignorable")
	}
}

func TestIsIgnorableCloseError_AlipayACQTradeNotExist(t *testing.T) {
	g := New(paymgr.NewManager())
	alipayErr := &paymgr.ChannelError{
		Channel: paymgr.ChannelAlipay,
		Code:    "ACQ.TRADE_NOT_EXIST",
	}
	if !g.IsIgnorableCloseError(orderflow.Channel(paymgr.ChannelAlipay), alipayErr) {
		t.Error("alipay ACQ.TRADE_NOT_EXIST should be ignorable")
	}

	// 其他 code 不可忽略
	alipayOther := &paymgr.ChannelError{
		Channel: paymgr.ChannelAlipay,
		Code:    "ACQ.SYSTEM_ERROR",
	}
	if g.IsIgnorableCloseError(orderflow.Channel(paymgr.ChannelAlipay), alipayOther) {
		t.Error("alipay SYSTEM_ERROR should not be ignorable")
	}
}

func TestIsIgnorableCloseError_OtherChannelChannelError(t *testing.T) {
	g := New(paymgr.NewManager())
	// 非支付宝渠道的 ChannelError 默认不可忽略（只 alipay 有特殊 code）
	wechatErr := &paymgr.ChannelError{
		Channel: paymgr.ChannelWechat,
		Code:    "ORDER_NOT_FOUND",
	}
	if g.IsIgnorableCloseError(orderflow.Channel(paymgr.ChannelWechat), wechatErr) {
		t.Error("wechat ChannelError should not be automatically ignorable")
	}
}

func TestIsIgnorableCloseError_GenericErrorNotIgnored(t *testing.T) {
	g := New(paymgr.NewManager())
	if g.IsIgnorableCloseError(orderflow.Channel("wechat"), errors.New("random failure")) {
		t.Error("generic error should not be ignorable")
	}
}

// 可选配置：WithTradeType 覆盖默认 TradeTypeApp
func TestWithTradeType(t *testing.T) {
	g := New(paymgr.NewManager(), WithTradeType(paymgr.TradeTypeJSAPI))
	if g.tradeType != paymgr.TradeTypeJSAPI {
		t.Errorf("tradeType = %v, want Jsapi", g.tradeType)
	}
}

func TestGateway_ValidateRejectsNilManager(t *testing.T) {
	g := New(nil)
	if err := g.Validate(); err == nil {
		t.Fatal("expected Validate to reject nil manager")
	}
}

func TestGateway_MethodsReturnErrorWhenManagerNil(t *testing.T) {
	g := New(nil)
	ctx := context.Background()

	cases := []struct {
		name string
		fn   func() error
	}{
		{
			name: "UnifiedOrder",
			fn: func() error {
				_, err := g.UnifiedOrder(ctx, "wechat", orderflow.UnifiedOrderRequest{
					OutTradeNo: "OUT-1", TotalAmount: 1, Subject: "x", NotifyURL: "https://example.com/notify",
				})
				return err
			},
		},
		{
			name: "CloseOrder",
			fn: func() error {
				return g.CloseOrder(ctx, "wechat", "OUT-1")
			},
		},
		{
			name: "QueryOrder",
			fn: func() error {
				_, err := g.QueryOrder(ctx, "wechat", "OUT-1")
				return err
			},
		},
		{
			name: "ParseNotify",
			fn: func() error {
				_, err := g.ParseNotify(ctx, "wechat", httptest.NewRequest("POST", "/notify", nil))
				return err
			},
		},
		{
			name: "AckNotify",
			fn: func() error {
				return g.AckNotify("wechat", httptest.NewRecorder())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("unexpected panic: %v", r)
					}
				}()
				err = tc.fn()
			}()
			if err == nil {
				t.Fatal("expected explicit error for nil manager")
			}
		})
	}
}
