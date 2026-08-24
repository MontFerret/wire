BUF = go run github.com/bufbuild/buf/cmd/buf@v1.72.0
BUF_BREAKING_AGAINST ?= .git#branch=main

.PHONY: build check-fmt check-generate check-tidy fmt generate proto-breaking proto-lint test test-race vet

build:
	go build ./...

fmt:
	go fmt ./...

check-fmt:
	test -z "$$(gofmt -l $$(find . -type f -name '*.go' -not -path './.git/*'))"

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
