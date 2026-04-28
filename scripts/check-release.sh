#!/usr/bin/env bash
# check-release.sh —— 发版前校验 driver 的发布就绪状态。
#
# 校验项：
#   1. driver 的 go.mod 不含 `replace github.com/gtkit/orderflow ...` 本地指令；
#   2. driver 的 go.mod 不 require `github.com/gtkit/orderflow v0.0.0` 占位符；
#   3. driver 的 go.sum 完整（GOWORK=off 下能跑 `go mod verify` + 编译探针）。
#
# 第 3 项是关键的"消费者视角"校验——本地开发期 workspace 的 replace 让 driver
# 不需要 go.sum 也能跑测试，但下游 `go get` 拿到 driver 后必须能独立编译。
# 漏掉 go.sum 条目会导致 CI 与下游用户构建失败。
#
# 用法：
#   scripts/check-release.sh         # 校验所有 driver
#   scripts/check-release.sh --fix   # 自动跑 GOWORK=off go mod tidy 修复缺失（谨慎使用）
#
# 退出码：0 通过；非 0 有遗漏

set -euo pipefail

cd "$(dirname "$0")/.."

mode="check"
if [[ "${1:-}" == "--fix" ]]; then
    mode="fix"
fi

failed=0

# ---- Step 1+2: go.mod 静态检查 ----
for mod in drivers/*/go.mod; do
    if [[ ! -f "$mod" ]]; then
        continue
    fi
    if grep -q "^replace[[:space:]]*github.com/gtkit/orderflow" "$mod"; then
        echo "ERROR: $mod still has local replace directive:"
        grep -n "replace.*github.com/gtkit/orderflow" "$mod" | sed 's/^/  /'
        failed=1
    fi
    if grep -qE "github.com/gtkit/orderflow v0\.0\.0" "$mod"; then
        echo "ERROR: $mod requires the v0.0.0 placeholder version:"
        grep -n "github.com/gtkit/orderflow v0\.0\.0" "$mod" | sed 's/^/  /'
        failed=1
    fi
done

# ---- Step 3: GOWORK=off 编译探针 ----
# `go test -run '^$' ./...` 不跑任何用例，仅触发编译 + 包解析。
# 若 go.sum 缺条目或 require 版本未发布，会立刻报错。
for dir in drivers/*/; do
    if [[ ! -f "$dir/go.mod" ]]; then
        continue
    fi
    name="$(basename "$dir")"
    if [[ "$mode" == "fix" ]]; then
        echo "FIX: $name -> go mod tidy"
        (cd "$dir" && GOWORK=off go mod tidy) || failed=1
    fi
    if ! (cd "$dir" && GOWORK=off go test -run '^$' ./... >/dev/null 2>&1); then
        echo "ERROR: $name fails to compile under GOWORK=off (likely missing go.sum entry)"
        echo "  fix: cd $dir && GOWORK=off go mod tidy"
        failed=1
    fi
done

if [[ $failed -eq 0 ]]; then
    echo "OK: all drivers are release-ready (no local replace, no v0.0.0, GOWORK=off compiles)"
fi

# ---- Step 4: 模块发版审计 ----
# 即使 driver 编译通过，也要看代码 / 依赖是否有更新但未发版的情况。
# 这是发版前最容易遗漏的一步——单跑 check-modules.sh 也行。
echo
echo "---- module release audit ----"
if ! bash "$(dirname "$0")/check-modules.sh" --quiet; then
    echo "ERROR: at least one module needs release (run scripts/check-modules.sh for details)"
    failed=1
fi

exit $failed
