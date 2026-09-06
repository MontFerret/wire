BUF = go run github.com/bufbuild/buf/cmd/buf@v1.72.0
BUF_BREAKING_AGAINST ?= .git#branch=main
DIR_BIN = ./bin
GOLANGCI_LINT_VERSION = v2.13.2
GOLANGCI_LINT_DIR = $(DIR_BIN)/tools/golangci-lint/$(GOLANGCI_LINT_VERSION)
GOLANGCI_LINT_SUFFIX := $(if $(filter windows,$(shell go env GOHOSTOS)),.exe)
GOLANGCI_LINT = $(GOLANGCI_LINT_DIR)/golangci-lint$(GOLANGCI_LINT_SUFFIX)

.PHONY: build check-fmt check-generate check-tidy fmt generate install-lint lint proto-breaking proto-lint test test-race vet

build:
	go build ./...

install-lint: $(GOLANGCI_LINT)

$(GOLANGCI_LINT):
	@set -eu; \
	lint_installer=$$(mktemp); \
	trap 'rm -f "$$lint_installer"' 0; \
	curl --fail --silent --show-error --location \
		"https://raw.githubusercontent.com/golangci/golangci-lint/$(GOLANGCI_LINT_VERSION)/install.sh" \
		--output "$$lint_installer"; \
	sh "$$lint_installer" -b "$(GOLANGCI_LINT_DIR)" "$(GOLANGCI_LINT_VERSION)"

fmt: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) fmt ./...

check-fmt: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) fmt --diff ./...

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) config verify && \
	$(GOLANGCI_LINT) run ./...

generate:
	$(BUF) generate

check-generate:
	@set -e; \
	tmp="$$(mktemp -d)"; \
	trap 'rm -rf "$$tmp"' EXIT; \
	cp -R gen "$$tmp/gen"; \
	$(BUF) generate; \
	diff -ru "$$tmp/gen" gen

proto-lint:
	$(BUF) lint

proto-breaking:
	$(BUF) breaking --against "$(BUF_BREAKING_AGAINST)"

check-tidy:
	go mod tidy -diff

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...
