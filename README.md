# Dispatch

Dispatch is a standalone MCP server that lets agents run commands asynchronously and inspect their complete, retained output precisely: run once, keep working, then read line ranges, follow output incrementally with cursors, or search with context — without flooding context or rerunning the command to recover lost logs.

- Exact argv, no implicit shell; explicit working directory and environment (inherit, set, unset), values never reported.
- Bounded waits (`wait=true` up to a deadline, or `wait_for_run`) separate from execution timeouts.
- stdout and stderr captured byte for byte, indexed by line, with a combined view; every response is bounded and says whether more is available, whether capture is still active, and whether anything was lost.
- Cancellation and timeouts terminate the whole process group; runs survive server restarts as records, and runs whose server died are reported as `interrupted`.

The full contract and acceptance criteria are in [docs/contract.md](docs/contract.md).

## Build

```sh
make build        # bin/dispatch
make check        # gofmt, vet, race-enabled tests including end-to-end MCP tests
```

Requires Go (see `go.mod`) on Linux or macOS.

## Use

Configure it as a stdio MCP server, for example in VS Code `.vscode/mcp.json`:

```json
{"servers": {"dispatch": {"type": "stdio", "command": "/path/to/bin/dispatch"}}}
```

Flags: `-state-dir` (default `$DISPATCH_STATE_DIR`, `$XDG_STATE_HOME/dispatch` or `~/.local/state/dispatch`), `-max-active`, `-max-output-bytes`, `-max-retained-runs`, `-max-retained-bytes`, `-retention`, `-kill-grace`, `-shutdown-timeout`, `-version`.

A typical loop:

1. `run_command {"command": "make", "args": ["test"], "cwd": "/repo", "wait": false}` → `run_id`
2. `wait_for_run {"run_id": "...", "timeout_seconds": 120}` → state, exit code, last lines
3. `search_output {"run_id": "...", "pattern": "FAIL", "context_lines": 5}`
4. `read_output {"run_id": "...", "stream": "stderr", "start_line": -50}`
