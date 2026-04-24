package orderflow

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// =============================================================================
// New / Config 验证
// =============================================================================

func TestNew_ReturnsErrMissingDepWhenAnyRequiredIsNil(t *testing.T) {
	// 基础：所有必填都存在时 New 应成功，作为对照
	baseCfg := Config[*testOrder]{
		Store:      newFakeStore(),
		Gateway:    newFakeGateway(),
		DelayQueue: newFakeDelayQueue(),
		Cache:      newFakeCache(),
		Stream:     newFakeStream(),
	}
	if _, err := New[*testOrder](baseCfg); err != nil {
		t.Fatalf("baseline New failed: %v", err)
	}

	// 每次抹掉一个必填，验证 ErrMissingDep 被正确报告
	cases := []struct {
		name    string
		mutator func(*Config[*testOrder])
	}{
		{"Store nil", func(c *Config[*testOrder]) { c.Store = nil }},
		{"Gateway nil", func(c *Config[*testOrder]) { c.Gateway = nil }},
		{"DelayQueue nil", func(c *Config[*testOrder]) { c.DelayQueue = nil }},
		{"Cache nil", func(c *Config[*testOrder]) { c.Cache = nil }},
		{"Stream nil", func(c *Config[*testOrder]) { c.Stream = nil }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := baseCfg
			c.mutator(&cfg)
			_, err := New[*testOrder](cfg)
			if !errors.Is(err, ErrMissingDep) {
				t.Fatalf("expected ErrMissingDep, got %v", err)
			}
		})
	}
}

func TestNew_RejectsNegativeOrderExpire(t *testing.T) {
	cfg := Config[*testOrder]{
		Store:       newFakeStore(),
		Gateway:     newFakeGateway(),
		DelayQueue:  newFakeDelayQueue(),
		Cache:       newFakeCache(),
		Stream:      newFakeStream(),
		OrderExpire: -time.Second,
	}
	_, err := New[*testOrder](cfg)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

type validatedGateway struct {
	*fakeGateway
	err error
}

func (g *validatedGateway) Validate() error {
	return g.err
}

func TestNew_RejectsDependencyValidateError(t *testing.T) {
	cfg := Config[*testOrder]{
		Store:      newFakeStore(),
		Gateway:    &validatedGateway{fakeGateway: newFakeGateway(), err: errors.New("manager is nil")},
		DelayQueue: newFakeDelayQueue(),
		Cache:      newFakeCache(),
		Stream:     newFakeStream(),
	}

	_, err := New[*testOrder](cfg)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
	if err == nil || err.Error() == ErrInvalidConfig.Error() {
		t.Fatalf("expected dependency details in error, got %v", err)
	}
}

type validatingStore struct {
	*fakeStore
	err error
}

func (s *validatingStore) Validate() error {
	return s.err
}

func TestNew_ReportsDependencyNameFromValidateError(t *testing.T) {
	cfg := Config[*testOrder]{
		Store:      &validatingStore{fakeStore: newFakeStore(), err: errors.New("db handle is nil")},
		Gateway:    newFakeGateway(),
		DelayQueue: newFakeDelayQueue(),
		Cache:      newFakeCache(),
		Stream:     newFakeStream(),
	}

	_, err := New[*testOrder](cfg)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "Store") {
		t.Fatalf("expected Store name in error, got %v", err)
	}
}

type validatingLocker struct {
	*fakeLocker
	err error
}

func (l *validatingLocker) Validate() error {
	return l.err
}

func TestNew_RejectsLockerValidateError(t *testing.T) {
	cfg := Config[*testOrder]{
		Store:      newFakeStore(),
		Gateway:    newFakeGateway(),
		DelayQueue: newFakeDelayQueue(),
		Cache:      newFakeCache(),
		Stream:     newFakeStream(),
		Locker:     &validatingLocker{fakeLocker: newFakeLocker(), err: errors.New("redis locker client is nil")},
	}

	_, err := New[*testOrder](cfg)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "Locker") {
		t.Fatalf("expected Locker name in error, got %v", err)
	}
}

func TestNew_DefaultOrderExpireApplied(t *testing.T) {
	env := newTestEnv(t)
	if got := env.engine.OrderExpire(); got != DefaultOrderExpire {
		t.Fatalf("default OrderExpire = %s, want %s", got, DefaultOrderExpire)
	}
}

func TestNew_GettersReturnInjectedValues(t *testing.T) {
	env := newTestEnv(t)
	if env.engine.DelayQueue() != env.dq {
		t.Error("DelayQueue() did not return the injected instance")
	}
	if env.engine.Logger() == nil {
		t.Error("Logger() returned nil; expected nop logger by default")
	}
	if env.engine.Location() == nil {
		t.Error("Location() returned nil")
	}
}

func TestResolveLocation(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantUTC bool
	}{
		{"empty -> Local", "", false},
		{"whitespace -> Local", "   ", false},
		{"invalid tz -> Local", "Not/A/Zone", false},
		{"UTC explicit", "UTC", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			loc := resolveLocation(c.in)
			if loc == nil {
				t.Fatal("resolveLocation returned nil")
			}
			if c.wantUTC && loc.String() != "UTC" {
				t.Errorf("got %s, want UTC", loc)
			}
		})
	}
}
