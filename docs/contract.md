# Dispatch contract

Dispatch is an MCP server (stdio) that runs commands asynchronously and keeps their complete output for bounded, precise inspection. It depends on no agent framework, repository layout, language or log format. Unix only (Linux, macOS).

## Tools

| Tool | Purpose |
| --- | --- |
| `run_command` | Start a command; optionally wait (bounded) for it. |
| `get_status` | State, timestamps, exit/signal, output counts, capture loss. |
| `wait_for_run` | Wait (bounded) for an existing run. |
| `read_output` | Bounded reads by line range, from the end, by cursor, or by byte offset. |
| `search_output` | Literal or RE2 search with context, paged by cursor. |
| `cancel_run` | Terminate a run's process group. |
| `list_runs` | Find retained runs, paged. |
| `delete_runs` | Explicitly remove finished runs and their output. |

Input and output JSON schemas are published by `tools/list`; the descriptions there are the user-facing reference.

## Execution

- `command` is executed directly with `args` as the exact argv (`argv[0]` is `command`). Nothing is interpreted by a shell; run a shell explicitly to use shell syntax.
- Lookup: a `command` containing `/` is a path (relative to `cwd`); a bare name is searched in the **child's** `PATH`, skipping empty and relative entries. No `PATH` in the child environment means a bare name fails to start.
- `cwd`: absolute, or relative to Dispatch's working directory; default Dispatch's working directory.
- stdin is `/dev/null`.
- Environment precedence: base = Dispatch's environment if `env.inherit` (default `true`), else empty; then `env.unset` keys are removed; then `env.set` is applied. A key in both `set` and `unset` is an invalid request. Changes affect only the child.
- Secrets: responses and `run.json` record only `inherit`, the names in `set` and `unset`, never values. Arguments are recorded and returned as given; do not put secrets in arguments.
- Each command runs in its own process group (its PID is the group ID). Descendants that stay in the group are owned by the run.

## States

| State | Meaning |
| --- | --- |
| `running` | Started; its process group has not been fully reaped. |
| `exited` | Ended. `exit_code` (normal exit) or `signal`; `termination` is `exit`, `signal`, `timeout`, `cancelled` or `server_shutdown`. |
| `failed_to_start` | Never ran (not found, not executable, bad `cwd`); `error` says why. It is still a run with an ID. |
| `interrupted` | The Dispatch instance that owned the run stopped (crash, kill) before it finished. `termination` is `unknown`. |

Invalid requests (bad parameters, conflicting env, over-limit values) are tool errors and create no run.

Timestamps are RFC 3339 UTC: `created_at`, `started_at`, `ended_at`.

## Waiting

- `run_command` requires `wait`. `false` returns at once. `true` waits until the run finishes or `wait_timeout_seconds` (default 30, max 300) passes; then `wait.deadline_reached` is `true` and the command continues.
- `wait_for_run` is the same bounded wait for an existing run.
- Waiting never returns the whole output: responses carry status and at most `tail_lines` (max 100, each line cut to 1 KiB, 8 KiB total) of the combined view.
- Cancelling an MCP request (or its client-side timeout) ends the wait, never the run.

## Timeouts, cancellation, cleanup of descendants

- `timeout_seconds` (execution timeout, default none) is independent of waiting. On expiry, or on `cancel_run`, Dispatch sends SIGTERM to the process group, then SIGKILL to the group if the leader has not exited after `kill_grace_seconds` (default 5, max 60).
- When the leader exits by itself, group members still running are given up to 2 s, then SIGTERM, then SIGKILL after the grace. `process_group` reports what was done. A run owns its group: nothing in it outlives the run.
- Processes that leave the group (`setsid`, double-fork into a new group) are not tracked. If such a process keeps the output pipes open, Dispatch closes them 2 s after the group ends and reports it in `capture_error`.
- Cancelling a finished run is a no-op that returns its status.

## Output capture

- stdout and stderr are captured through separate pipes, independently of any reader, and stored byte for byte (`stdout.log`, `stderr.log`).
- **Lines** end at `\n`. Text excludes the `\n`; `\r` is kept. A stream's last line without `\n` is reported with `unterminated: true` once the stream closes. While capture is active, bytes after the last `\n` form an in-progress line, shown in stdout/stderr views with `partial: true`; it is not counted in `total_lines` and cursors do not move past it.
- **Text decoding**: lines are decoded as UTF-8; invalid bytes are replaced with U+FFFD and the line is flagged `invalid_utf8`. Exact bytes are always available with byte reads (`byte_offset`, `encoding: base64`). Text byte reads never split a UTF-8 sequence at a chunk boundary.
- **Ordering**: each stream's byte and line order is exact. The `combined` view lists complete lines of both streams in the order Dispatch received them. Because the streams are separate pipes, the order **across** streams is approximate: it never reorders lines within one stream, but it cannot prove which of two lines on different streams was written first.
- **Limits**: each run stores at most `max_output_bytes` (default and maximum: the server's `-max-output-bytes`, 256 MiB) for both streams together. Beyond it, Dispatch keeps reading so the command never blocks, counts the bytes in `lost_bytes`, and sets `limit_reached`. The command is not killed.
- **Capture failures** (write errors such as a full disk, unreadable pipes) set `capture_error`; later bytes are counted in `lost_bytes`. Output is never discarded silently: `output.complete` is `true` only when capture finished with nothing lost.

## Reading and searching

- Views: `stdout`, `stderr`, `combined` (default). Line numbers are 1-based and stable for a view.
- Position: `start_line` (negative counts from the end) or `cursor` (`<view>:<line>`, returned as `next_cursor`). A cursor names the next line to read; repeated reads with `next_cursor` return every complete line exactly once, including lines captured later.
- Bounds: `max_lines` (default 200, max 2000), `max_bytes` of text (default 32 KiB, max 256 KiB), `max_line_bytes` (default 4 KiB). A longer line is returned with `truncated`, its full `bytes` and its `byte_offset` for byte reads.
- `more_available`: output exists past this response now. `response_truncated`: the response's bounds left something out. Neither is loss. Loss is reported only by `capture.lost_bytes`, `capture.limit_reached` and `capture.error`, repeated in every read and search result.
- Search: literal by default, RE2 with `regex: true` (linear time), `ignore_case`. Up to `max_matches` (default 20, max 200) matching lines with `context_lines` (default 2, max 20); overlapping contexts merge into one group. `scan_complete` and `next_cursor` page through the rest; a cursor at the end resumes later to search only new lines. Lines longer than 1 MiB are searched in their first 1 MiB (`long_lines_partially_searched`). In-progress partial lines are not searched.

## Persistence, restart and retention

- State directory: `-state-dir`, else `$DISPATCH_STATE_DIR`, else `$XDG_STATE_HOME/dispatch`, else `~/.local/state/dispatch`. Each run is `runs/<run_id>/` with `run.json`, the logs and the line indexes, all private to the user (0600/0700).
- Run IDs are `r-YYYYMMDDtHHMMSS-xxxxxxxx` (UTC time plus random hex) and never reused.
- While a run is active its owning Dispatch process holds an exclusive lock on `owner.lock`. Liveness is decided by that lock, never by a stored PID: if the lock is free and `run.json` still says `running`, the owner died, and the run becomes `interrupted` (its indexes are repaired; output captured so far stays readable). Dispatch never signals an interrupted run's PID after a restart, because the PID may belong to another process. Its processes may still exist; `error` says so.
- Several Dispatch instances may share a state directory. A run owned by another live instance is readable (`owned_elsewhere: true`) but can only be cancelled through that instance.
- Server shutdown (stdin closed by the client, client disconnect, SIGINT, SIGTERM): Dispatch terminates every active run (`termination: server_shutdown`) as `cancel_run` does, records them, and exits; `-shutdown-timeout` (15 s) bounds the wait.
- Retention: finished runs are kept until deleted with `delete_runs` or removed automatically, oldest first, when they are older than `-retention` (7 days), more than `-max-retained-runs` (500), or larger in total than `-max-retained-bytes` (4 GiB). Running runs are never removed. Readers holding a removed run's files keep reading them.

## Resource bounds

- At most `-max-active` (16) running commands; more is a tool error (`too many active runs`), with no queueing.
- Per-run stored output ≤ `-max-output-bytes`; command plus args ≤ 1 MiB.
- Every response is bounded by its tool's limits above; `list_runs` pages at most 100 runs; `delete_runs` takes at most 100 IDs.

## Acceptance criteria

Each is exercised against real subprocesses through a real MCP client (the Go SDK's `CommandTransport` against the built binary) in [e2e/](../e2e), or by unit tests in [internal/capture](../internal/capture).

| # | Criterion | Test |
| --- | --- | --- |
| A1 | Arguments reach the child exactly (empty, spaces, quotes, `$`, `*`, newline, Unicode); no shell. | `TestArgsPreservedExactly`, `TestExplicitShell` |
| A2 | `wait=true` returns a finished status with exit code and bounded tail; `wait=false` returns a running run ID at once. | `TestRunWaitAndRead`, `TestWaitFalseThenWaitForRun` |
| A3 | A wait deadline returns the current state and the command continues. | `TestRunCommandWaitDeadline`, `TestWaitFalseThenWaitForRun` |
| A4 | Non-zero exit is reported with its stderr retained; an unstartable command is `failed_to_start` with a reason. | `TestCommandFailure`, `TestFailedToStart` |
| A5 | 200 000 lines are retained exactly (byte count), reads are bounded and say `more_available`, without loss. | `TestHighVolumeOutput` |
| A6 | Incremental cursor reads during a run see every line once; partial lines are flagged and not skipped. | `TestIncrementalReadsWhileRunning`, `TestPartialLineAndUnterminatedEnd` |
| A7 | Search returns matches with merged context and pages to the exact total. | `TestSearchWithContextAndPaging` |
| A8 | Concurrent runs keep separate, complete output; the active limit is enforced. | `TestConcurrentRuns`, `TestMaxActiveRuns` |
| A9 | Execution timeout terminates the process group including a grandchild, escalating to SIGKILL. | `TestExecutionTimeoutKillsProcessGroup`, `TestTimeoutEscalatesToSIGKILL` |
| A10 | Cancel terminates descendants; repeat cancel is a no-op; leftover group members are terminated after the leader exits. | `TestCancelKillsDescendants`, `TestLeftoverGroupMembersAreTerminated` |
| A11 | Storage limits and write failures are reported as lost bytes / capture errors, never silently. | `TestCaptureLimitIsReported`, `TestWriterLimitCountsLostBytes`, `TestWriterSurfacesWriteFailure` |
| A12 | Binary output is retrievable byte-exact; invalid UTF-8 is flagged in text. | `TestBinaryOutputAndByteReads` |
| A13 | Environment precedence (inherit, unset, set), isolation, and no values in responses or `run.json`. | `TestEnvironmentPrecedenceAndSecrecy` |
| A14 | Cancelling an MCP request does not affect the run. | `TestWaitRequestCancellationLeavesRunAlone` |
| A15 | After a server crash, a running run is `interrupted` with its output readable; finished runs survive unchanged. | `TestRestartMarksInterruptedRuns`, `TestRepairIndexesTornState` |
| A16 | Client disconnect shuts the server down, terminating run descendants, recorded as `server_shutdown`. | `TestShutdownTerminatesRuns` |
| A17 | Explicit cleanup deletes finished runs only and rejects malformed IDs. | `TestDeleteRuns` |
