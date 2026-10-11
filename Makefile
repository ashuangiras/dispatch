.PHONY: build check fmt-check vet test clean

BIN := bin/dispatch

# build: the binary agents configure as an MCP server.
build:
	go build -o $(BIN) ./cmd/dispatch

# check: the gate. Tests run uncached (-count=1) because the e2e tests build
# and run the binary and real subprocesses, which Go's test cache cannot see.
check: fmt-check vet test

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi; echo "fmt-check: PASS"

vet:
	go vet ./...

test:
	go test -race -count=1 ./...

clean:
	rm -rf bin
