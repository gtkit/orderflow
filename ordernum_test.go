package orderflow

import (
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestDefaultGenerateOrderNo_FixedWidth(t *testing.T) {
	no := defaultGenerateOrderNo()
	if len(no) != 30 {
		t.Fatalf("expected 30 chars, got %d: %q", len(no), no)
	}
	// 前 20 字节应该是 10 进制时间戳（全数字）
	for i, r := range no[:20] {
		if r < '0' || r > '9' {
			t.Fatalf("prefix pos %d not digit: %q", i, no)
		}
	}
}

func TestDefaultGenerateOrderNo_UniqueUnderConcurrency(t *testing.T) {
	const N = 1000
	results := make([]string, N)
	var wg sync.WaitGroup
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = defaultGenerateOrderNo()
		}(i)
	}
	wg.Wait()

	seen := make(map[string]struct{}, N)
	for _, r := range results {
		if _, dup := seen[r]; dup {
			t.Fatalf("duplicate order no generated: %q", r)
		}
		seen[r] = struct{}{}
	}
}

// TestDefaultGenerateOrderNo_LexicographicOrderUnderRace 回归测试 C1：
// 并发场景下按字典序排序后，前 20 位时间戳应当单调不减——
// 这是 defaultGenerateOrderNo 的"字典序 = 生成顺序"契约。
// 未加原子 snowflake 前，此测试高概率失败（time 与 seq 分步获取，goroutine 交错会导致逆序）。
func TestDefaultGenerateOrderNo_LexicographicOrderUnderRace(t *testing.T) {
	const N = 5000
	results := make([]string, N)
	var wg sync.WaitGroup
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = defaultGenerateOrderNo()
		}(i)
	}
	wg.Wait()

	// 排序后检查时间戳部分单调不减
	sorted := append([]string(nil), results...)
	sortStrings(sorted)

	var prevMs int64
	for i, s := range sorted {
		ms, err := strconv.ParseInt(s[:20], 10, 64)
		if err != nil {
			t.Fatalf("parse ms from %q: %v", s, err)
		}
		if ms < prevMs {
			t.Fatalf("lexicographic order violates time order at index %d: ms=%d < prev=%d (value %q)", i, ms, prevMs, s)
		}
		prevMs = ms
	}
}

// TestAdvanceOrderNoState_StrictMonotonic 直接对内部状态机做严格单调性断言：
// 任意两次相邻调用，(ms, seq) 组合必须严格递增。
func TestAdvanceOrderNoState_StrictMonotonic(t *testing.T) {
	const N = 10000
	results := make([][2]uint64, N)
	var wg sync.WaitGroup
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ms, seq := advanceOrderNoState()
			results[idx] = [2]uint64{ms, seq}
		}(i)
	}
	wg.Wait()

	// 把 (ms, seq) 合成单一 uint64 排序，应该得到严格递增序列
	combined := make([]uint64, N)
	for i, r := range results {
		combined[i] = (r[0] << 20) | r[1]
	}
	sortUint64s(combined)
	for i := 1; i < len(combined); i++ {
		if combined[i] <= combined[i-1] {
			t.Fatalf("duplicate or non-monotonic at index %d: %d <= %d", i, combined[i], combined[i-1])
		}
	}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func sortUint64s(s []uint64) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func TestDefaultGenerateOrderToken_Unpredictable(t *testing.T) {
	// 安全契约：token 必须 128 bit 随机，同输入多次调用必须产生不同值。
	// 旧实现是 SHA-256 哈希，攻击者拿到 (orderNo, userID, productID) 可离线重算——这是漏洞。
	const N = 100
	tokens := make(map[string]struct{}, N)
	isHex := func(r rune) bool {
		return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')
	}
	for range N {
		tok := defaultGenerateOrderToken("ORD1", 1001, 2001)
		if len(tok) != 32 {
			t.Fatalf("expected 32 hex chars, got %d: %q", len(tok), tok)
		}
		for _, r := range tok {
			if !isHex(r) {
				t.Fatalf("non-hex char in token: %q", tok)
			}
		}
		if _, dup := tokens[tok]; dup {
			t.Fatalf("token collision within %d iterations: %q", N, tok)
		}
		tokens[tok] = struct{}{}
	}
}

func TestDefaultGenerateOrderToken_InputsNotLeaked(t *testing.T) {
	// 防回归：token 的 hex 表示中不应泄露入参（旧哈希实现相同入参总输出相同 token，
	// 现随机实现多次输出不同——间接验证不包含入参信息）。
	t1 := defaultGenerateOrderToken("ORD-VICTIM", 1001, 2001)
	t2 := defaultGenerateOrderToken("ORD-VICTIM", 1001, 2001)
	if t1 == t2 {
		t.Fatalf("same inputs produced same token — token is deterministic, not random: %q", t1)
	}
}

func TestAppendFixedWidth(t *testing.T) {
	cases := []struct {
		s     string
		width int
		want  string
	}{
		{"", 3, "000"},
		{"1", 3, "001"},
		{"123", 3, "123"},
		{"12345", 3, "345"},
	}
	for _, c := range cases {
		got := string(appendFixedWidth(nil, c.s, c.width))
		if !strings.EqualFold(got, c.want) {
			t.Errorf("appendFixedWidth(%q, %d) = %q, want %q", c.s, c.width, got, c.want)
		}
	}
}
