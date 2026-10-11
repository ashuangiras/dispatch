# Agent guide

- Build: `make build` (writes `bin/dispatch`). Gate: `make check` (gofmt, vet, `go test -race -count=1 ./...`; the e2e tests build the binary and drive it over MCP with real subprocesses, so tests always run uncached).
- Go: the version in `go.mod`. No other pinned tools.
- Contracts and acceptance criteria: [docs/contract.md](docs/contract.md). Keep Dispatch generic: no build systems, languages or log formats baked in.
- Development procedures and verification evidence live in Polaroid (`http://127.0.0.1:7417/mcp`, see `.vscode/mcp.json`), repository `github.com/ashuangiras/dispatch`: list its bindings (`verify-change`, `hosted-ci`) and resolve them before verifying a commit.
- Do not push, publish releases or trigger hosted CI without the owner's authorization (`hosted-ci` policy: local-only).
