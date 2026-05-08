#!/usr/bin/env bash
# lint-all.sh —— 多 module 仓库 lint 入口。
#
# 背景：仓库根 `golangci-lint run ./...` 只扫根模块，drivers/* 是独立 module
# 完全不会被扫到（且会因 "no go files to analyze" 报错）。本脚本逐 module
# cd + 跑 lint，是真实的"全仓 lint"入口。
#
# 用法：
#   scripts/lint-all.sh           # 跑所有 module 的 lint，全量输出
#   scripts/lint-all.sh --quiet   # 仅在有 issue 时输出对应 module
#
# 退出码：
#   0  全部 module lint 通过
#   1  至少一个 module 有 issue（脚本会列出哪些）
#
# 触发场景（强制使用）：
#   - 准备 commit / tag / push 前（CLAUDE.md / AGENTS.md 发版 checklist 第 5 项）
#   - 修改任何 .go 源码后
#   - CI 流水线
#
# 与 check-release.sh 的关系：check-release.sh 末尾会调用本脚本，发版前
# 一并校验 lint 状态——避免"假绿"（从仓库根跑 ./... 看到 0 issues 就以为通过）。

set -euo pipefail

cd "$(dirname "$0")/.."

quiet=0
if [[ "${1:-}" == "--quiet" ]]; then
    quiet=1
fi

# ========== 模块定义 ==========
# 与 check-modules.sh 的 MODULES 保持一致；复制而非共享，避免脚本之间隐式耦合。
MODULES=(
    "orderflow|."
    "gormstore|drivers/gormstore"
    "paymgrgw|drivers/paymgrgw"
    "rediscache|drivers/rediscache"
    "rediszq|drivers/rediszq"
)

if ! command -v golangci-lint >/dev/null 2>&1; then
    echo "ERROR: golangci-lint not found in PATH" >&2
    echo "  install: https://golangci-lint.run/welcome/install/" >&2
    exit 2
fi

failed=0
failing_modules=()

if [[ $quiet -eq 0 ]]; then
    echo "=================================================="
    echo "  Multi-module Lint"
    echo "  golangci-lint: $(golangci-lint version --short 2>/dev/null || golangci-lint version 2>&1 | head -1)"
    echo "=================================================="
    echo
fi

for entry in "${MODULES[@]}"; do
    IFS='|' read -r name path <<< "$entry"

    # 跳过不存在的路径（如 driver 目录被删）
    if [[ ! -f "$path/go.mod" ]]; then
        continue
    fi

    # 用子 shell 隔离 cd，避免影响主流程
    if (cd "$path" && GOWORK=off golangci-lint run ./... > /tmp/lint-$$.out 2>&1); then
        if [[ $quiet -eq 0 ]]; then
            printf "  [✓] %-12s  %s\n" "$name" "$path"
        fi
    else
        failed=1
        failing_modules+=("$name|$path")
        if [[ $quiet -eq 0 ]]; then
            printf "  [✗] %-12s  %s  ISSUES\n" "$name" "$path"
        else
            echo "[$name] lint failed:"
        fi
        sed 's/^/      /' /tmp/lint-$$.out
        echo
    fi
done

rm -f /tmp/lint-$$.out

if [[ $quiet -eq 0 ]]; then
    echo "=================================================="
    if [[ $failed -eq 0 ]]; then
        echo "  ✓ All modules pass lint."
    else
        echo "  ✗ Modules with lint issues:"
        for line in "${failing_modules[@]}"; do
            IFS='|' read -r n p <<< "$line"
            echo "    - $n ($p)"
        done
        echo
        echo "  Reproduce locally:"
        for line in "${failing_modules[@]}"; do
            IFS='|' read -r n p <<< "$line"
            echo "    cd $p && GOWORK=off golangci-lint run ./..."
        done
    fi
    echo "=================================================="
fi

exit $failed
