PKG := $(shell go list ./... | grep -v '/mock$$')

print-pkg:
	@echo $(PKG)
test:
	gotestsum --format testname $(PKG)
testw:
	gotestsum --watch --format testname $(PKG)
lint:
	golangci-lint run
clean:
	rm -rf .coverage
	rm -rf ./pkg/**/mock
	rm -rf .tmp/**
setup:
	make import-tools
	make mockgen
coverage:
	mkdir -p .coverage
	go test -coverprofile=.coverage/coverage.out $(PKG)
	go tool cover -html=.coverage/coverage.out -o .coverage/coverage.html
mockgen:
	go generate ./...
fmt:
	go fmt $(PKG)
import-tools:
	go install gotest.tools/gotestsum@v1.12.0
	go install go.uber.org/mock/mockgen@v0.4.0
