#!/usr/bin/env bash
# check-modules.sh —— 多 module 仓库发版审计：检查每个子包是否需要发版。
#
# 检测项（每个模块独立判断）：
#   1. 该模块自上次 tag 以来是否有代码变更
#   2. 该模块的直接依赖是否有可用更新（含 indirect 排除）
#   3. driver 子模块的 require github.com/gtkit/orderflow 是否对齐核心包最新 tag
#
# 用法：
#   scripts/check-modules.sh           # 输出每个模块状态
#   scripts/check-modules.sh --quiet   # 仅在需要发版时输出
#
# 退出码：
#   0  全部模块都已最新（无需发版）
#   1  至少一个模块需要发版（脚本会列出具体原因）
#
# 触发场景（强制使用）：
#   - 完成阶段性工作准备 commit 前
#   - 升级了任何子包依赖（如 go mod tidy 后）
#   - 用户问"还有什么要做"时
#   - 任何对子包代码 / go.mod / go.sum 的修改后

set -euo pipefail

cd "$(dirname "$0")/.."

# shellcheck source=scripts/modules.sh
source "$(dirname "$0")/modules.sh"

quiet=0
if [[ "${1:-}" == "--quiet" ]]; then
    quiet=1
fi

# ========== 工具函数 ==========

# 找出某模块的最新 tag（按 SemVer 排序，取头）
latest_tag() {
    local prefix="$1"
    git tag -l "${prefix}[0-9]*" --sort=-v:refname | head -1
}

# 判断模块自 tag 以来是否有代码变更
# 排除：非代码文件（.DS_Store / coverage.out）、其他模块路径、辅助文件
has_code_changes() {
    local tag="$1"
    local path="$2"

    # 没有 tag → 视为"全是变更"（首次发版）
    if [[ -z "$tag" ]]; then
        return 0
    fi

    local files
    if [[ "$path" == "." ]]; then
        # 根模块：排除 drivers/* + 各类辅助目录 + 非代码产物
        files=$(git diff --name-only "$tag" HEAD -- \
            ':!drivers/*' \
            ':!.github/*' \
            ':!.openspec-auto/*' \
            ':!.openspec-auto-backup/*' \
            ':!.harness/*' \
            ':!.agents/*' \
            ':!.codex/*' \
            ':!.claude/*' \
            ':!coverage.out' \
            ':!**/.DS_Store' \
            ':!go.work' \
            ':!go.work.sum' \
            2>/dev/null | head -1)
    else
        files=$(git diff --name-only "$tag" HEAD -- "$path" 2>/dev/null | head -1)
    fi

    [[ -n "$files" ]]
}

# 列出某模块自 tag 以来变更的文件（用于人工审阅）
list_changes() {
    local tag="$1"
    local path="$2"
    if [[ -z "$tag" ]]; then
        echo "(no tag yet — initial release pending)"
        return
    fi
    if [[ "$path" == "." ]]; then
        git diff --name-only "$tag" HEAD -- \
            ':!drivers/*' ':!.github/*' ':!.openspec-auto/*' ':!.openspec-auto-backup/*' \
            ':!.harness/*' ':!.agents/*' ':!.codex/*' ':!.claude/*' \
            ':!coverage.out' ':!**/.DS_Store' ':!go.work' ':!go.work.sum' \
            2>/dev/null | head -10 | sed 's/^/      /'
    else
        git diff --name-only "$tag" HEAD -- "$path" 2>/dev/null | head -10 | sed 's/^/      /'
    fi
}

# 列出某模块的直接依赖更新（排除 indirect）
list_dep_updates() {
    local path="$1"
    (cd "$path" && GOWORK=off go list -u -m -f '{{if and .Update (not .Indirect)}}{{.Path}}: {{.Version}} -> {{.Update.Version}}{{end}}' all 2>/dev/null | grep -v '^$' || true)
}

# 检查 driver 的 require orderflow 是否对齐最新核心包 tag
require_orderflow_version() {
    local path="$1"
    if [[ "$path" == "." ]]; then return; fi
    (cd "$path" && GOWORK=off go list -m -f '{{if not .Indirect}}{{.Version}}{{end}}' github.com/gtkit/orderflow 2>/dev/null)
}

# ========== 主流程 ==========

needs_release=0
report_lines=()

ROOT_TAG=$(latest_tag "v")

if [[ $quiet -eq 0 ]]; then
    echo "=================================================="
    echo "  Multi-module Release Audit"
    echo "  Repo: $(basename "$PWD")"
    echo "  Core latest tag: ${ROOT_TAG:-<none>}"
    echo "=================================================="
    echo
fi

for entry in "${MODULES[@]}"; do
    IFS='|' read -r name path prefix <<< "$entry"

    # 跳过不存在的路径（如 driver 目录被删）
    if [[ ! -f "$path/go.mod" ]]; then
        continue
    fi

    tag=$(latest_tag "$prefix")
    issues=()

    # 检查 1：代码变更
    if has_code_changes "$tag" "$path"; then
        if [[ -z "$tag" ]]; then
            issues+=("no tag yet (initial release pending)")
        else
            issues+=("code changed since $tag")
        fi
    fi

    # 检查 2：直接依赖更新
    dep_updates=$(list_dep_updates "$path")
    if [[ -n "$dep_updates" ]]; then
        # 多行更新汇总到一行
        dep_summary=$(echo "$dep_updates" | tr '\n' ';' | sed 's/;$//')
        issues+=("direct dep updates: $dep_summary")
    fi

    # 检查 3：driver 的 require orderflow 是否对齐最新核心 tag
    if [[ "$path" != "." ]] && [[ -n "$ROOT_TAG" ]]; then
        req_ver=$(require_orderflow_version "$path" || true)
        if [[ -z "$req_ver" ]]; then
            issues+=("missing direct require github.com/gtkit/orderflow")
        elif [[ "$req_ver" != "$ROOT_TAG" ]]; then
            issues+=("require orderflow $req_ver != latest core $ROOT_TAG")
        fi
    fi

    if [[ ${#issues[@]} -eq 0 ]]; then
        if [[ $quiet -eq 0 ]]; then
            printf "  [✓] %-12s  %s  up to date\n" "$name" "${tag:-<none>}"
        fi
    else
        needs_release=1
        # 用 NUL 字节连接 issues，summary 按 NUL split（避免空格被错误拆分）
        joined_issues=$(printf '%s\x1f' "${issues[@]}")
        report_lines+=("$name|$path|${tag:-<none>}|${joined_issues}")
        if [[ $quiet -eq 0 ]]; then
            printf "  [⚠] %-12s  %s  RELEASE NEEDED\n" "$name" "${tag:-<none>}"
            for i in "${issues[@]}"; do
                printf "      - %s\n" "$i"
            done
            # 列出代码变更文件（仅前 10 个）便于人工审阅
            if [[ "${issues[*]}" == *"code changed"* ]] || [[ "${issues[*]}" == *"initial release"* ]]; then
                printf "    changed files:\n"
                list_changes "$tag" "$path"
            fi
        fi
    fi
done

if [[ $quiet -eq 0 ]]; then
    echo
    echo "=================================================="
    if [[ $needs_release -eq 0 ]]; then
        echo "  ✓ All modules are up to date. No release needed."
    else
        echo "  ⚠ Modules needing release:"
        for line in "${report_lines[@]}"; do
            IFS='|' read -r n p t joined <<< "$line"
            echo "    - $n ($p) [$t]:"
            # joined 用 \x1f 分隔多个 reason
            while IFS= read -d $'\x1f' -r r; do
                [[ -n "$r" ]] && echo "        • $r"
            done <<< "$joined"
        done
        echo
        echo "  Next steps:"
        echo "    1. Determine the SemVer bump for each module (patch/minor/major)"
        echo "    2. Update CHANGELOG.md if applicable"
        echo "    3. Tag with: git tag -a <prefix>vX.Y.Z -m \"...\""
        echo "    4. Push tags: git push <remote> <tag>"
    fi
    echo "=================================================="
fi

exit $needs_release
