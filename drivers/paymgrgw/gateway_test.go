package paymgrgw

import (
	"errors"
	"fmt"
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
