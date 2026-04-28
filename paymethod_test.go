package orderflow_test

import (
	"testing"

	"github.com/gtkit/orderflow"
)

func TestPayMethod_String(t *testing.T) {
	tests := []struct {
		name string
		p    orderflow.PayMethod
		want string
	}{
		{"零值未选择", 0, "未选择"},
		{"微信支付", orderflow.PayMethodWechat, "微信支付"},
		{"支付宝", orderflow.PayMethodAlipay, "支付宝支付"},
		{"银联", orderflow.PayMethodUnion, "银联支付"},
		{"未知值", 99, "未知"},
		{"负值视作未知", -1, "未知"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.String(); got != tt.want {
				t.Errorf("PayMethod(%d).String() = %q, want %q", tt.p, got, tt.want)
			}
		})
	}
}

func TestPayMethod_NumericValues(t *testing.T) {
	if int8(orderflow.PayMethodWechat) != 1 {
		t.Errorf("PayMethodWechat = %d, want 1", orderflow.PayMethodWechat)
	}
	if int8(orderflow.PayMethodAlipay) != 2 {
		t.Errorf("PayMethodAlipay = %d, want 2", orderflow.PayMethodAlipay)
	}
	if int8(orderflow.PayMethodUnion) != 3 {
		t.Errorf("PayMethodUnion = %d, want 3", orderflow.PayMethodUnion)
	}
}
