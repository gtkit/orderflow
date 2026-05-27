.PHONY: help vet test bench cover lint lint-all audit readiness check release-check dry-release release tidy clean delcommit gittag

.DEFAULT_GOAL := help

REMOTE ?= gtkit

ifneq ($(filter release dry-release,$(MAKECMDGOALS)),)
  ifeq ($(strip $(VERSION)),)
    $(error VERSION is required, e.g. make $(firstword $(MAKECMDGOALS)) VERSION=v1.13.0)
  endif
endif

help: ## 显示可用 target
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ================== 质量门(根模块) ==================

vet: ## go vet(根模块)
	GOWORK=off go vet ./...

test: ## race 测试 + count=1(根模块)
	GOWORK=off go test -race -count=1 -timeout=5m ./...

bench: ## Benchmark + allocs(根模块)
	GOWORK=off go test -run=^$$ -bench=. -benchmem -count=3 ./...

cover: ## 覆盖率(根模块)
	GOWORK=off go test -coverprofile=coverage.out ./...

lint: ## golangci-lint(仅根模块,drivers/* 不会被扫到)
	GOWORK=off golangci-lint run ./...

# ================== 多 module 跨仓 ==================

lint-all: ## 多 module lint(根 + drivers/*)
	bash scripts/lint-all.sh

audit: ## 多 module 发版审计(检测每个子包是否需要发版)
	bash scripts/check-modules.sh

readiness: ## driver readiness + lint-all + audit
	bash scripts/check-release.sh

check: readiness ## 别名: readiness(等价于 bash scripts/check-release.sh)

# ================== 发版 ==================

## release-check 是 tag 前必跑质量门(全局规则 #4 checklist)
release-check: ## tag 前质量门(vet + race/coverage + bench + readiness)
	GOWORK=off go vet ./...
	GOWORK=off go test -race -count=1 -covermode=atomic -coverprofile=coverage.out -timeout=5m ./...
	GOWORK=off go test -run=^$$ -bench=. -benchmem -count=3 ./...
	bash scripts/check-release.sh --skip-audit
	@ echo "✅ release-check 全部通过"

## dry-release 仅打印发版计划,不创建 tag、不修改文件
## 用法: make dry-release VERSION=v1.13.0
dry-release: ## 发版 dry-run(需 VERSION=vX.Y.Z)
	bash scripts/release-all.sh $(VERSION) --remote $(REMOTE)

## release 创建并推送根 tag + drivers 对齐 + drivers tag
## 用法: make release VERSION=v1.13.0
release: release-check ## 一键发版(需 VERSION=vX.Y.Z,先跑 release-check)
	bash scripts/release-all.sh $(VERSION) --push --remote $(REMOTE)

# ================== 杂项 ==================

tidy: ## go mod tidy(根 + drivers/*)
	GOWORK=off go mod tidy
	@for d in drivers/*/; do echo ">>> $$d"; (cd "$$d" && GOWORK=off go mod tidy) || exit 1; done

clean: ## 清理生成产物
	rm -f coverage.out

gittag: ## 显示最新根 tag
	git tag --sort=-version:refname | grep -vE '^drivers/' | head -1

delcommit: ## 删除最近一次提交,但保留修改内容
	git reset --soft HEAD~1
