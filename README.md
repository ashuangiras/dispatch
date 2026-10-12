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
2. `wait_for_output` can wait for a readiness/log pattern before the command exits; `wait_for_run {"run_id": "...", "timeout_seconds": 120}` → state, exit code, last lines
3. `search_output {"run_id": "...", "pattern": "FAIL", "context_lines": 5}`
4. `read_output {"run_id": "...", "stream": "stderr", "start_line": -50}`

## Operational notes

- **Permissions and environment.** Commands run with the Dispatch server's own OS permissions, user and (by default) its environment; Dispatch does not sandbox or drop privileges. Only run it where you already trust what it is asked to run.
- **Secrets.** `run.json` and every response record only argument values and environment variable *names*, never environment *values* — but arguments are recorded and returned verbatim. Do not pass secrets as command-line arguments; use `env.set` instead.
- **Cancellation.** Cancelling the MCP request behind `run_command(wait=true)` or `wait_for_run` (including a client-side timeout) ends that wait only; the command keeps running. Only `cancel_run` terminates the command itself.
- **Disconnect and restart.** A graceful stop (the client closes stdin, disconnects, or sends SIGINT/SIGTERM) terminates every active run first, recorded as `server_shutdown`, bounded by `-shutdown-timeout`. A crash (the process is killed outright) does not: active runs become `interrupted` on the next start against the same `-state-dir`, their outcome unknown, and Dispatch never signals their old PID (it may belong to a different process by then).
- **Capture limits and retention.** Each run stores at most `-max-output-bytes` (default 256 MiB) for both streams together; bytes beyond that are counted in `lost_bytes`, never silently dropped, and the command is never blocked or killed for it. Finished runs are removed automatically, oldest first, once they exceed `-retention` (age), `-max-retained-runs` (count) or `-max-retained-bytes` (total size); a running run is never removed. See [docs/contract.md](docs/contract.md) for the complete contract.
