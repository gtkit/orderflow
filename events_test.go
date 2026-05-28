package orderflow

import "testing"

func TestAnomalyKindValid(t *testing.T) {
	tests := []struct {
		name string
		kind AnomalyKind
		want bool
	}{
		{name: "known", kind: AnomalyPaidOnCancelled, want: true},
		{name: "unknown", kind: AnomalyKind("paid_on_cancelled_typo"), want: false},
		{name: "empty", kind: AnomalyKind(""), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mustEqual(t, tt.kind.Valid(), tt.want, "Valid")
		})
	}
}
