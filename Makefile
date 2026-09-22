BUF = go run github.com/bufbuild/buf/cmd/buf@v1.72.0
BUF_BREAKING_AGAINST ?= .git#branch=main
DIR_BIN = ./bin
GOLANGCI_LINT_VERSION = v2.13.2
GOLANGCI_LINT_DIR = $(DIR_BIN)/tools/golangci-lint/$(GOLANGCI_LINT_VERSION)
GOLANGCI_LINT_SUFFIX := $(if $(filter windows,$(shell go env GOHOSTOS)),.exe)
GOLANGCI_LINT = $(GOLANGCI_LINT_DIR)/golangci-lint$(GOLANGCI_LINT_SUFFIX)

.PHONY: build check-ferret-tidy check-fmt check-generate check-tidy fmt generate install-lint lint proto-breaking proto-lint test test-ferret test-ferret-race test-race vet

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
	cd test/ferret && GOWORK=off "$(abspath $(GOLANGCI_LINT))" fmt --config ../../.golangci.yml ./...

check-fmt: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) fmt --diff ./...
	cd test/ferret && GOWORK=off "$(abspath $(GOLANGCI_LINT))" fmt --config ../../.golangci.yml --diff ./...

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) config verify && \
	$(GOLANGCI_LINT) run ./...
	cd test/ferret && GOWORK=off "$(abspath $(GOLANGCI_LINT))" run --config ../../.golangci.yml ./...

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

check-ferret-tidy:
	cd test/ferret && GOWORK=off go mod tidy -diff

test:
	go test ./...

test-race:
	go test -race ./...

test-ferret:
	cd test/ferret && GOWORK=off go test -timeout=2m ./...

test-ferret-race:
	cd test/ferret && GOWORK=off go test -race -timeout=2m ./...

vet:
	go vet ./...
	cd test/ferret && GOWORK=off go vet ./...
