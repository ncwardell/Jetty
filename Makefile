# Local targets mirror .github/workflows/ci.yml so `make check` is the same
# gate CI runs. If you change one, change the other.

GO ?= go

.PHONY: help fmt fmt-check vet build test test-race cover generate docs check clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

fmt: ## Format all Go sources
	gofmt -w ./agent ./main.go

fmt-check: ## Fail if any source is unformatted
	@unformatted=$$(gofmt -l ./agent ./main.go); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt'd:"; echo "$$unformatted"; exit 1; \
	fi

vet: ## Run go vet
	$(GO) vet ./...

build: ## Build all packages
	$(GO) build ./...

test: ## Run tests
	$(GO) test ./...

test-race: ## Run tests with the race detector
	$(GO) test -race ./...

cover: ## Run tests and report total coverage
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -func=coverage.out | tail -1

generate: ## Sync the embedded dashboard from web-ui/
	$(GO) generate ./...

check: fmt-check vet build test-race ## Everything CI runs

clean: ## Remove build and coverage artifacts
	rm -f jetty coverage.out
