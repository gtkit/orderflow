package rediszq

import "github.com/gtkit/orderflow"

// 编译期断言：*Queue 必须满足 orderflow.DelayQueue。
// 核心包接口一旦调整导致不兼容，rediszq 的 CI 会在这里直接失败，避免下游使用方先踩坑。
var _ orderflow.DelayQueue = (*Queue)(nil)
