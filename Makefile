BUF = go run github.com/bufbuild/buf/cmd/buf@v1.72.0

.PHONY: build check-generate generate proto-lint test vet

build: vet test

fmt:
	go fmt ./...

generate:
	$(BUF) generate

check-generate:
	$(BUF) generate
	git diff --exit-code -- gen
	test -z "$$(git status --porcelain --untracked-files=all -- gen)"

proto-lint:
	$(BUF) lint

test:
	go test ./...

vet:
	go vet ./...
