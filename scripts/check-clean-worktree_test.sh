#!/usr/bin/env bash
# 测试 release-check 工作树清洁守卫。

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
guard="$repo_root/scripts/check-clean-worktree.sh"

tmpdir="$(mktemp -d)"
cleanup() {
    rm -rf "$tmpdir"
}
trap cleanup EXIT

copy_guard() {
    local repo="$1"
    mkdir -p "$repo/scripts"
    cp "$guard" "$repo/scripts/check-clean-worktree.sh"
}

init_repo() {
    local repo="$1"
    mkdir -p "$repo"
    git -C "$repo" init -q
    git -C "$repo" config user.email "test@example.invalid"
    git -C "$repo" config user.name "Release Test"
    printf 'ok\n' >"$repo/tracked.txt"
    copy_guard "$repo"
    git -C "$repo" add tracked.txt scripts/check-clean-worktree.sh
    git -C "$repo" commit -q -m "初始化"
}

assert_success() {
    local name="$1"
    shift
    if ! "$@" >"$tmpdir/$name.out" 2>"$tmpdir/$name.err"; then
        echo "FAIL: $name 应成功" >&2
        cat "$tmpdir/$name.out" "$tmpdir/$name.err" >&2
        exit 1
    fi
}

assert_failure() {
    local name="$1"
    shift
    if "$@" >"$tmpdir/$name.out" 2>"$tmpdir/$name.err"; then
        echo "FAIL: $name 应失败" >&2
        cat "$tmpdir/$name.out" "$tmpdir/$name.err" >&2
        exit 1
    fi
}

clean_repo="$tmpdir/clean"
init_repo "$clean_repo"
assert_success clean bash "$clean_repo/scripts/check-clean-worktree.sh"

dirty_repo="$tmpdir/dirty"
init_repo "$dirty_repo"
printf 'dirty\n' >>"$dirty_repo/tracked.txt"
assert_failure dirty bash "$dirty_repo/scripts/check-clean-worktree.sh"
grep -q 'tracked.txt' "$tmpdir/dirty.err"

staged_repo="$tmpdir/staged"
init_repo "$staged_repo"
printf 'staged\n' >"$staged_repo/staged.txt"
git -C "$staged_repo" add staged.txt
assert_failure staged bash "$staged_repo/scripts/check-clean-worktree.sh"
grep -q 'staged.txt' "$tmpdir/staged.err"

untracked_repo="$tmpdir/untracked"
init_repo "$untracked_repo"
printf 'untracked\n' >"$untracked_repo/untracked.txt"
assert_failure untracked bash "$untracked_repo/scripts/check-clean-worktree.sh"
grep -q 'untracked.txt' "$tmpdir/untracked.err"

skip_repo="$tmpdir/skip"
init_repo "$skip_repo"
printf 'dirty\n' >>"$skip_repo/tracked.txt"
assert_success skip env ALLOW_DIRTY_RELEASE_CHECK=1 bash "$skip_repo/scripts/check-clean-worktree.sh"
grep -q '已跳过工作树清洁检查' "$tmpdir/skip.out"

echo "PASS: check-clean-worktree"
