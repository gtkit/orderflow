#!/usr/bin/env bash
# check-release.sh —— 发版前校验 driver 的 go.mod 不含 replace github.com/gtkit/orderflow 指令。
#
# 背景：本地开发期 driver 的 go.mod 用 `replace github.com/gtkit/orderflow => ../..`
# 指向工作目录，打 tag 后必须删除并 require 真实版本号，否则下游 `go get` 会失败或
# 被诱导手动修改 go.mod 引入不受信任的 fork。
#
# 用法：
#   scripts/check-release.sh         # 校验所有 driver
#   scripts/check-release.sh --fix   # 不实现；手动删 replace 并更新 require 版本号
#
# 退出码：0 通过；非 0 有遗漏

set -euo pipefail

cd "$(dirname "$0")/.."

failed=0
for mod in drivers/*/go.mod; do
    if [[ ! -f "$mod" ]]; then
        continue
    fi
    if grep -q "^replace[[:space:]]*github.com/gtkit/orderflow" "$mod"; then
        echo "ERROR: $mod still has local replace directive:"
        grep -n "replace.*github.com/gtkit/orderflow" "$mod" | sed 's/^/  /'
        failed=1
    fi
    # 同时校验 require 的 orderflow 版本不是 v0.0.0 占位符
    if grep -qE "github.com/gtkit/orderflow v0\.0\.0" "$mod"; then
        echo "ERROR: $mod requires the v0.0.0 placeholder version:"
        grep -n "github.com/gtkit/orderflow v0\.0\.0" "$mod" | sed 's/^/  /'
        failed=1
    fi
done

if [[ $failed -eq 0 ]]; then
    echo "OK: all drivers' go.mod are release-ready (no local replace, no v0.0.0 require)"
fi

exit $failed
