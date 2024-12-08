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
coverage:
	mkdir -p .coverage
	go test -coverprofile=.coverage/coverage.out $(PKG)
	go tool cover -html=.coverage/coverage.out -o .coverage/coverage.html
mockgen:
	find ./pkg -name 'interface.go' -exec sh -c 'for file; do \
		dest_dir=$$(dirname "$$file")/mock; \
		mkdir -p "$$dest_dir"; \
		mockgen -source="$$file" -destination="$$dest_dir/$$(basename "$$file")" -package=mock; \
	done' sh {} +
fmt:
	go fmt $(PKG)
