#!/usr/bin/env bash
# 检查 release-check 是否运行在干净工作树上。

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${ALLOW_DIRTY_RELEASE_CHECK:-}" == "1" ]]; then
    echo "⚠ 已跳过工作树清洁检查：ALLOW_DIRTY_RELEASE_CHECK=1"
    exit 0
fi

if [[ -z "$(git status --porcelain)" ]]; then
    exit 0
fi

echo "ERROR: release-check requires a clean git worktree." >&2
echo "Dirty paths:" >&2
git status --porcelain | sed -n '1,50p' >&2
echo >&2
echo "请先提交、stash 或清理这些变更；临时验证可显式设置 ALLOW_DIRTY_RELEASE_CHECK=1。" >&2
exit 1
