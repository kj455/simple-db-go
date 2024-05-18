PKG ?= ./...

test:
	go test -v $(PKG)
testw:
	gow test $(PKG)
lint:
	golangci-lint run
coverage:
	go test -coverprofile=.coverage/coverage.out $(PKG)
	go tool cover -html=.coverage/coverage.out -o .coverage/coverage.html
fmt:
	go fmt $(PKG)

PHONY: test testw lint coverage fmt
