#!/usr/bin/env bash
# release-all.sh —— 统一版本号的多 module 发版脚本。
#
# Dry-run（不创建 tag、不提交）：
#   scripts/release-all.sh v1.10.0
#
# 完整发布：
#   scripts/release-all.sh v1.10.0 --push
#
# 完整发布会：
#   1. 创建并推送根 tag vX.Y.Z；
#   2. 将所有 driver 的 require github.com/gtkit/orderflow 对齐到 vX.Y.Z；
#   3. 如有 driver go.mod/go.sum 变化，自动提交并推送；
#   4. 创建并推送所有 driver tag。

set -euo pipefail

cd "$(dirname "$0")/.."

# shellcheck source=scripts/modules.sh
source "$(dirname "$0")/modules.sh"

usage() {
    cat <<'USAGE'
Usage:
  scripts/release-all.sh vX.Y.Z [--push] [--yes] [--remote <name>]

Examples:
  scripts/release-all.sh v1.10.0
  scripts/release-all.sh v1.10.0 --push
  scripts/release-all.sh v1.10.0 --push --yes
  scripts/release-all.sh v1.10.0 --push --remote origin

Default mode is dry-run: it validates release preconditions and prints the
release plan. Use --push to create annotated tags, align driver core requires,
commit generated driver go.mod/go.sum changes when needed, and push everything.
In --push mode, type the exact version at the confirmation prompt, or pass --yes
only from trusted automation after reviewing the printed plan.
USAGE
}

version=""
remote="gtkit"
push=0
assume_yes=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --push)
            push=1
            shift
            ;;
        --yes)
            assume_yes=1
            shift
            ;;
        --remote)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --remote requires a value" >&2
                usage >&2
                exit 2
            fi
            remote="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        v*)
            if [[ -n "$version" ]]; then
                echo "ERROR: version specified more than once" >&2
                usage >&2
                exit 2
            fi
            version="$1"
            shift
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "$version" ]]; then
    echo "ERROR: version is required" >&2
    usage >&2
    exit 2
fi

if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
    echo "ERROR: invalid version '$version' (want vX.Y.Z or vX.Y.Z-prerelease)" >&2
    exit 2
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "ERROR: not inside a git work tree" >&2
    exit 2
fi

if ! git remote get-url "$remote" >/dev/null 2>&1; then
    echo "ERROR: git remote '$remote' not found" >&2
    exit 2
fi

branch="$(git branch --show-current)"
if [[ -z "$branch" ]]; then
    echo "ERROR: detached HEAD is not allowed for release" >&2
    exit 2
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "ERROR: working tree is not clean; commit or stash changes before release" >&2
    git status --short
    exit 2
fi

# Refresh tags and remote branch state before deciding whether a tag exists.
git fetch --tags "$remote" >/dev/null

upstream="$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null || true)"
if [[ -n "$upstream" ]]; then
    local_head="$(git rev-parse HEAD)"
    upstream_head="$(git rev-parse "$upstream")"
    if [[ "$local_head" != "$upstream_head" ]]; then
        echo "ERROR: local HEAD ($local_head) does not match upstream $upstream ($upstream_head)" >&2
        exit 2
    fi
else
    remote_ref="refs/remotes/$remote/$branch"
    if git show-ref --verify --quiet "$remote_ref"; then
        local_head="$(git rev-parse HEAD)"
        remote_head="$(git rev-parse "$remote_ref")"
        if [[ "$local_head" != "$remote_head" ]]; then
            echo "ERROR: local HEAD ($local_head) does not match $remote/$branch ($remote_head)" >&2
            exit 2
        fi
    else
        echo "ERROR: cannot find upstream or $remote/$branch for branch sync check" >&2
        exit 2
    fi
fi

version_no_v="${version#v}"
if ! grep -qE "^## \[${version_no_v}\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$" CHANGELOG.md; then
    echo "ERROR: CHANGELOG.md lacks release heading for [$version_no_v]" >&2
    echo "       expected: ## [$version_no_v] - YYYY-MM-DD" >&2
    exit 2
fi

root_tag="$version"
driver_tags=()
for path in "${DRIVER_MODULES[@]}"; do
    if [[ ! -f "$path/go.mod" ]]; then
        echo "ERROR: module path '$path' has no go.mod" >&2
        exit 2
    fi
    driver_tags+=("$path/$version")
done

all_tags=("$root_tag" "${driver_tags[@]}")
for tag in "${all_tags[@]}"; do
    if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
        echo "ERROR: local tag already exists: $tag" >&2
        exit 2
    fi
    if git ls-remote --exit-code --tags "$remote" "refs/tags/$tag" >/dev/null 2>&1; then
        echo "ERROR: remote tag already exists: $tag" >&2
        exit 2
    fi
done

echo "==> Release plan for $version"
echo "  root tag: $root_tag"
echo "  driver tags:"
printf '    %s\n' "${driver_tags[@]}"
echo "  remote: $remote"

if [[ $push -eq 0 ]]; then
    echo
    echo "DRY-RUN: no tags were created and no files were changed."
    echo "Re-run with --push after committing release files."
    exit 0
fi

confirm_release_push() {
    if [[ $assume_yes -eq 1 ]]; then
        echo
        echo "CONFIRMED: --yes supplied; proceeding with irreversible tag push."
        return
    fi
    if [[ "${RELEASE_CONFIRM:-}" == "$version" ]]; then
        echo
        echo "CONFIRMED: RELEASE_CONFIRM=$version"
        return
    fi
    echo
    echo "WARNING: --push will create and push annotated tags to '$remote'."
    echo "Remote tags must not be renamed, deleted, or force-overwritten after publication."
    echo "Type the exact version '$version' to continue:"
    read -r answer
    if [[ "$answer" != "$version" ]]; then
        echo "ERROR: release confirmation mismatch; aborting before tag creation" >&2
        exit 2
    fi
}

confirm_release_push

message="版本 $version

主要变更：
- release: 统一发布 orderflow 与 drivers 子模块 $version

破坏性变更（如有）：
- 无

相关 Issue：无"

echo
echo "==> Creating and pushing root tag $root_tag"
git tag -a "$root_tag" -m "$message"
git push "$remote" "$root_tag"

echo
echo "==> Aligning driver require github.com/gtkit/orderflow $version"
for path in "${DRIVER_MODULES[@]}"; do
    echo "  $path"
    (cd "$path" && GOWORK=off go get "github.com/gtkit/orderflow@$version" && GOWORK=off go mod tidy)
done

if [[ -n "$(git status --porcelain -- drivers)" ]]; then
    git add drivers/*/go.mod drivers/*/go.sum
    git commit -m "chore(drivers): 对齐 orderflow $version"
    git push "$remote" "$branch"
fi

echo
echo "==> Creating local driver tags"
created_driver_tags=()
cleanup_driver_tags() {
    if [[ ${#created_driver_tags[@]} -gt 0 ]]; then
        git tag -d "${created_driver_tags[@]}" >/dev/null 2>&1 || true
    fi
}
trap cleanup_driver_tags ERR
for tag in "${driver_tags[@]}"; do
    git tag -a "$tag" -m "$message"
    created_driver_tags+=("$tag")
done

echo
echo "==> Running release checks"
bash scripts/check-release.sh --skip-audit

trap - ERR

echo
echo "==> Pushing driver tags"
git push "$remote" "${driver_tags[@]}"

echo
echo "==> Release tags pushed"
printf '  %s\n' "${all_tags[@]}"
