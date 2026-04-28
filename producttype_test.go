package orderflow_test

import (
	"testing"

	"github.com/gtkit/orderflow"
)

func TestProductType_String(t *testing.T) {
	tests := []struct {
		name string
		p    orderflow.ProductType
		want string
	}{
		{"零值未指定", 0, "未指定"},
		{"文本", orderflow.ProductTypeText, "文本"},
		{"视频", orderflow.ProductTypeCourse, "视频"},
		{"音频", orderflow.ProductTypeColumn, "音频"},
		{"会员", orderflow.ProductTypeMembership, "会员"},
		{"未知值", 50, "未知"},
		{"负值视作未知", -1, "未知"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.String(); got != tt.want {
				t.Errorf("ProductType(%d).String() = %q, want %q", tt.p, got, tt.want)
			}
		})
	}
}

func TestProductType_NumericValues(t *testing.T) {
	if int8(orderflow.ProductTypeText) != 1 {
		t.Errorf("ProductTypeText = %d, want 1", orderflow.ProductTypeText)
	}
	if int8(orderflow.ProductTypeCourse) != 2 {
		t.Errorf("ProductTypeCourse = %d, want 2", orderflow.ProductTypeCourse)
	}
	if int8(orderflow.ProductTypeColumn) != 3 {
		t.Errorf("ProductTypeColumn = %d, want 3", orderflow.ProductTypeColumn)
	}
	if int8(orderflow.ProductTypeMembership) != 99 {
		t.Errorf("ProductTypeMembership = %d, want 99", orderflow.ProductTypeMembership)
	}
}
