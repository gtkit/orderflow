package orderflow

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

// 单机单进程严格单调的订单号状态：
//
//	高 44 位 = 毫秒时间戳（足够表达到公元 2527 年）
//	低 20 位 = 毫秒内自增序列（每毫秒上限 ~100 万）
//
// 通过 CAS 循环保证并发下 (ms, seq) 作为整体原子推进。
// 字典序 = 生成顺序的不变量仅在单进程内成立——多进程部署必须通过
// Config.GenerateOrderNo 注入带机器 ID 的生成器。
const (
	orderNoSeqBits = 20
	orderNoSeqMask = (1 << orderNoSeqBits) - 1
)

var orderNoState atomic.Uint64

// DefaultGenerateOrderNo 返回包默认订单号。
//
// 行为与 Config.GenerateOrderNo 为 nil 时 Engine 使用的生成器一致：
// 当前格式为 <20 位毫秒时间戳><6 位 36 进制自增序列><4 位随机后缀>。
// 返回值固定 30 字节。单进程内字典序等于生成顺序；多进程部署若需要机器 ID，
// 仍应通过 Config.GenerateOrderNo 注入自定义生成器。
//
// userID 参数用于保持与 GenerateOrderNoFunc 签名一致；默认实现会忽略该参数。
func DefaultGenerateOrderNo(userID int64) string {
	return defaultGenerateOrderNo(userID)
}

// defaultGenerateOrderNo 是 Config.GenerateOrderNo 的默认实现。
//
// 格式：<20 位毫秒时间戳><6 位 36 进制自增序列><4 位随机后缀>
// 长度固定 30 位。**单机单进程下字典序 = 生成顺序**（原子 CAS 推进 (ms, seq) 整体状态）；
// 多机部署下仅保证唯一性（靠 4 位随机后缀提供碰撞防护），字典序不严格。
//
// 入参 userID 由签名约定保留但默认实现忽略——业务方需要"订单号含 user_id"
// 时应注入自定义 GenerateOrderNoFunc，不要依赖默认。
func defaultGenerateOrderNo(_ int64) string {
	ms, seq := advanceOrderNoState()

	var randBuf [2]byte
	// ⚠ STDLIB CONTRACT GUARD（AGENTS.md 允许的 panic 例外）：
	// Go 1.24+ crypto/rand.Read 的契约是"永远返回 nil err"（失败时它在 stdlib 内部
	// 自己 panic）。这里的 panic 是**契约失败兜底**——若未来 stdlib 违反承诺把 err
	// 透出来，宁可立即崩溃也不要静默生成弱熵订单号（弱订单号会污染对账与防重）。
	// 触发概率近似 0；不要把这条路径误认为业务 panic。
	if _, err := rand.Read(randBuf[:]); err != nil {
		panic(fmt.Errorf("orderflow: crypto/rand.Read failed: %w", err))
	}

	// 预分配：20 + 6 + 4 = 30 字节
	buf := make([]byte, 0, 30)
	buf = appendFixedWidth(buf, strconv.FormatUint(ms, 10), 20)
	buf = appendFixedWidth(buf, strconv.FormatUint(seq, 36), 6)
	buf = appendFixedWidth(buf, hex.EncodeToString(randBuf[:]), 4)
	return string(buf)
}

// advanceOrderNoState 原子推进 (ms, seq) 状态并返回本次占用的值。
//
// CAS 循环保证任意两个并发调用者拿到的 (ms, seq) 严格有序——即字典序 = 生成顺序。
// 时钟回拨或同毫秒内 seq 递增；seq 溢出（同一毫秒内超过 2^20 次调用）会主动推进 ms。
func advanceOrderNoState() (ms, seq uint64) {
	for {
		old := orderNoState.Load()
		oldMs := old >> orderNoSeqBits
		nowMs := uint64(time.Now().UnixMilli())

		var next uint64
		switch {
		case nowMs > oldMs:
			// 正常跨毫秒：推进到新毫秒，seq 归零
			next = nowMs << orderNoSeqBits
		default:
			// 同毫秒或时钟回拨：只递增 seq；若 seq 溢出，主动推进 ms
			next = old + 1
			if next&orderNoSeqMask == 0 {
				next = ((oldMs + 1) << orderNoSeqBits)
			}
		}

		if orderNoState.CompareAndSwap(old, next) {
			return next >> orderNoSeqBits, next & orderNoSeqMask
		}
	}
}

// defaultGenerateOrderToken 是 Config.GenerateOrderToken 的默认实现。
//
// **安全设计**：返回 16 字节 crypto/rand 的 hex 编码（128 bit 熵），**不使用**入参。
// 早期版本用 SHA-256(orderNo|userID|productID) 截断——确定性哈希意味着任何拿到三元组
// 的人（对账文件、客服工单、监控日志）都能离线重算 token。新实现是真正不可预测。
//
// 入参 (orderNo, userID, productID) 保留在签名里是为了兼容：业务可用 Config.GenerateOrderToken
// 注入自己的实现，譬如用 HMAC(secret, 三元组) 实现"带服务端密钥的确定性 token"——这是
// 另一种合法选择，但不应是默认。
func defaultGenerateOrderToken(_ string, _ int64, _ uint64) string {
	var buf [16]byte
	// ⚠ STDLIB CONTRACT GUARD（AGENTS.md 允许的 panic 例外）：
	// 同 defaultGenerateOrderNo——`crypto/rand.Read` 契约是"绝不返回 err"。这里的
	// panic 是契约失败兜底，避免静默生成弱熵 token（会让 PollStatus / Subscribe 鉴权
	// 沦为摆设）。触发概率近似 0；不要误认为业务 panic。
	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Errorf("orderflow: crypto/rand.Read failed: %w", err))
	}
	return hex.EncodeToString(buf[:])
}

// appendFixedWidth 在 s 左侧用 '0' 补足 / 右侧截断到 width 宽度后追加到 dst。
func appendFixedWidth(dst []byte, s string, width int) []byte {
	if len(s) >= width {
		return append(dst, s[len(s)-width:]...)
	}
	for range width - len(s) {
		dst = append(dst, '0')
	}
	return append(dst, s...)
}
