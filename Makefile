PKG := $(shell go list ./... | grep -v '/mock$$')

print-pkg:
	@echo $(PKG)
test:
	go test -v $(PKG)
testw:
	gow test -timeout 5s $(PKG)
lint:
	golangci-lint run
coverage:
	go test -coverprofile=.coverage/coverage.out $(PKG)
	go tool cover -html=.coverage/coverage.out -o .coverage/coverage.html
mockgen:
	find ./pkg -name '*.go' ! -name '*_test.go' ! -path './pkg/**/mock/*' -exec sh -c 'for file; do \
		dest_dir=$$(dirname "$$file")/mock; \
		mkdir -p "$$dest_dir"; \
		mockgen -source="$$file" -destination="$$dest_dir/$$(basename "$$file")" -package=mock; \
	done' sh {} +
fmt:
	go fmt $(PKG)

PHONY: test testw lint coverage fmt
