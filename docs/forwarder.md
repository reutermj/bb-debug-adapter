# DAP Relay — Worker-Side Forwarder

> Status: **Draft for refinement.** This is the §7.4 follow-up. It specifies the
> worker-side forwarder, which bridges a debuggable action's DAP server to the
> relay. It builds directly on the **edge-client** (`edge-client.md`) as
> `side = FORWARDER` and on the §5.3 decisions. Section references like "§5.3"
> point at `high-level-design.md`; "edge-client §X" points at `edge-client.md`;
> "protocol §X" at `relay-protocol.md`.

## 1. Role and placement

The relay-facing forwarder lives **inside `bb_worker`**, in the per-thread
executor stack, running **concurrently with the blocking `Runner.Run`** call
(§5.3). For a debuggable action it bridges the action's **stdio** — delivered over
a Unix-domain socket in the build directory — to an embedded edge-client
(`side = FORWARDER`), which does all the protocol work. The forwarder itself only
adds the worker-specific glue: deciding debuggability, setting up the build-dir
socket, the `Runner.Run` goroutine, and terminating the action when the debugger
leaves.

`bb_runner` gains one small, **network-egress-free** step (§5.3): when the action
is debuggable it dials the worker-provided build-directory socket and wires the
action's stdio to it. It owns the action's fds, so the stdio bridge necessarily
passes through it; but it never touches the network — the relay client stays in
the worker. DAP's stdio transport is delivered as inherited fds, so it works
through `chroot` and any network sandboxing unchanged.

## 2. Triggering: debuggability (§5.5)

An action is debuggable **iff** its `Command.environment_variables` contains
`BB_DEBUG_SESSION_ID` (§5.1, §5.5). The executor reads this from the `Command` it
already loads to run the action. If absent, the forwarder does nothing and
execution is entirely unchanged. If present, its value is the `session_key`.

## 3. Stdio wiring over a build-directory socket (§5.3)

For a debuggable action:

1. **Worker:** the per-thread executor creates a **Unix-domain socket in the build
   directory** (a path it already controls, alongside the stdout/stderr log paths
   it passes today), starts listening, and passes the socket's path to the runner
   in a new, optional `RunRequest` field — set **only** when the action is
   debuggable. No port, no range, no env injection.
2. **Runner:** when that path is present, `bb_runner` **dials** the socket and
   wires the action's stdio to the resulting connection
   (`pkg/runner/local_runner.go`):
   - `cmd.Stdin = conn` — the client→server DAP bytes the action reads.
   - `cmd.Stdout = io.MultiWriter(stdoutFile, conn)` — a **tee**: the action's
     stdout is both captured to the normal `ActionResult` log *and* mirrored to the
     debug channel (the server→client DAP bytes).
   - `cmd.Stderr` is **unchanged** — a normal log channel for adapter diagnostics.

   When the action is not debuggable the field is absent and the runner behaves
   exactly as today (stdin `/dev/null`, stdout/stderr to log files).

**The action's contract.** A debuggable action **is**, or launches, a DAP adapter
that speaks DAP over **stdin/stdout** — the native DAP transport, e.g. `lldb-dap`
run with no special flags. That is the user's concern (a launch wrapper, etc.),
not the forwarder's. The one requirement: **stdout must carry pure DAP framing** —
the adapter must own stdout and emit the debuggee's own program output as DAP
`output` events; a stray write to stdout (from a wrapper, the shell, or the
debuggee) would interleave into the frame stream and corrupt it. `stderr` is free
for diagnostics.

## 4. Lifecycle

```
 listen(build-dir UDS) ─▶ Runner.Run(ctx) ──────────────────── blocks ──▶ returns
        │                       │ (concurrent)                              │
        │                       ▼                                           │
        │   runner dials UDS, wires action stdio, spawns action             │
        ▼                       │                                           ▼
 forwarder goroutine:           ▼                                    (action exited)
   Accept() ──connected──▶ run edge-client(side=FORWARDER, session_key, action conn)
        │                                                       │
        └── action exits before accept ──▶ give up              ▼ terminal outcome
                                                          (§5 termination)
```

1. **Start** (debuggable only): create and listen on the build-directory socket
   (§3), pass its path into `RunRequest`, and spawn the forwarder goroutine
   alongside `Runner.Run`.
2. **Accept** the runner's connection — deterministic, no retry loop: the worker
   listens *before* `Run`, and the runner dials as it sets up the action's stdio,
   so the accept resolves once. (If the action exits before its stdio is ever
   wired — e.g. it failed to start — the accept simply never completes and the
   forwarder gives up when `Run` returns.) A proxy that connected first has its
   messages buffered at the relay until the forwarder attaches
   (store-and-forward, §5.4).
3. **Bridge**: hand the accepted connection to the edge-client (edge-client §11)
   with `side = FORWARDER`, `session_key` from `BB_DEBUG_SESSION_ID`, the worker's
   relay access (§6), and a close-reason source (§5.1). The edge-client runs the
   protocol until a terminal outcome.

## 5. Termination

The forwarder ends when either the action exits or the debugger leaves; these are
the two drivers, and they meet in the middle.

### 5.1 Action exits first (normal / debugged-to-completion)

`Runner.Run` returns (action completed, or was killed). The worker tears down the
forwarder. The action — and thus its DAP adapter — is gone (its stdout closed, so
the forwarder's connection sees EOF), so this is a **local close**
(edge-client §9.1): the edge-client emits a terminal
`Close{PROCESS_EXITED, exit_code, detail}` as the final `S2C` frame, where
`exit_code` comes from the `Runner.Run` result. The edge-client flushes that
`Close` to the relay (best-effort; see §7) so the proxy — and the developer's DAP
client — sees a clean end, with whatever `terminated`/`exited` the adapter itself
emitted already having flowed through beforehand (§5.4).

### 5.2 Debugger leaves first → terminate the action

The proxy's terminal `Close` reaches the forwarder as an inbound `Close`, so the
edge-client returns **`PeerClosed`** (edge-client §9.2). On that outcome the
worker **terminates the action immediately** (§5.4, "Terminate the action on
debug-session-end") — using the
cancellation machinery it already has for action timeouts. `Runner.Run` then
returns and teardown proceeds as in §5.1 (now with a killed exit status). No
grace period, no timer; attach-style "detach and keep running" is a non-goal.

### 5.3 Never connected

If the action exits before its stdio is ever wired (it failed to start) or never
produces a DAP adapter (no adapter, early crash), the forwarder's accept never
completes — or completes but the connection closes immediately — and it gives up
when `Run` returns. Any proxy waiting on the relay never gets a peer and is
eventually reaped by the relay's orphan timeout (protocol §8). The action's own
result is unaffected.

## 6. Reaching the relay

The forwarder dials **`bb_dap_relay` directly**, not through the `bb_storage`
frontend (§5.2 permits this for internal farm components). The worker holds one
relay gRPC client (configured per §7), and each forwarder opens an `Attach` stream
on it via the edge-client. This reuses the worker's existing pattern of holding
outbound gRPC clients to the farm.

## 7. Configuration

New worker configuration:

- **Relay endpoint + credentials** — gRPC client config for dialing
  `bb_dap_relay` (endpoint, TLS, keepalive, edge-client §10). Its presence enables
  worker-side debug forwarding; absent ⇒ the worker ignores `BB_DEBUG_SESSION_ID`
  entirely and never asks the runner to wire stdio.

There is **no** port-range configuration, and therefore no range-overlap or
ephemeral-range startup validation: the build-directory socket path is allocated
per action (like the existing stdout/stderr log paths), so there is nothing to
coordinate across runners or workers on a host.

## 8. Interaction with normal execution — best-effort coupling left open

The forwarder is a goroutine beside `Runner.Run`; the action executes
independently of it. If the relay is unreachable, the edge-client keeps retrying
(reconnect backoff, edge-client §8) for as long as the action's DAP connection is
open, and the action is **not** blocked on it.

Whether a debug-relay failure should ever surface as an **action failure** is
**deliberately left open**: a debug session exists to
debug, so a broken
relay is arguably a broken run — but we are not committing to strong isolation
*or* strong coupling for the MVP. Concretely the MVP does the simple, natural
thing — forwarder problems are logged and do not fail the action, and on action
exit the worker does not block the result indefinitely on flushing the terminal
`Close` (§5.1) — and we leave the coupling policy to revisit later rather than
engineering guarantees around it now.

## 9. Code changes and hook points

- **Read `BB_DEBUG_SESSION_ID`** from `Command.environment_variables` in the
  per-thread executor to decide debuggability (§2). The action's environment is
  otherwise **unchanged** (no `BB_DEBUG_PORT` injection).
- **New optional `RunRequest` field** for the build-directory debug-socket path
  (`pkg/proto/runner/runner.proto`), set only for debuggable actions — additive,
  mirroring the existing `stdout_path`/`stderr_path` string fields.
- **Wire the action's stdio in `bb_runner`** (`pkg/runner/local_runner.go`): when
  the field is set, dial the socket and set `cmd.Stdin = conn`,
  `cmd.Stdout = io.MultiWriter(stdoutFile, conn)`, keep the conn open across the
  run, and close it on exit (clean EOF) (§3). This is the only runner change.
- **Worker side**: create/listen on the build-dir socket, pass its path in
  `RunRequest`, and **accept** the runner's connection for the edge-client (§3–§4).
- **Wrap `Runner.Run`** (the `runner.Run` call in `local_build_executor.go`) so the
  forwarder goroutine runs concurrently and is torn down when `Run` returns, and so
  a `PeerClosed` outcome can cancel the run (§5.2).
- **Construct the relay client** once per worker from config (§7), shared across
  threads.

Everything protocol-facing (sequencing, acks, retain/replay, dedupe, reconnect,
framing, the terminal `Close`) is the **edge-client's**, not duplicated here.

## 10. Open questions

- **Best-effort coupling (open)** — see §8; the action-vs-debug-failure
  coupling policy is intentionally unresolved.
- **Tee backpressure (open)** — `io.MultiWriter(stdoutFile, conn)` blocks on its
  slower sink, so a stalled relay could backpressure the action's stdout. Negligible
  at debug volume; if it matters, buffer the debug-channel write asynchronously
  (DAP bytes must never be dropped, so the buffer grows or blocks — it cannot
  discard). Impl detail, not an MVP blocker.
- **Forwarder close reason when the stdio stream dies but the action lives** — if
  the action's adapter closes/garbles stdout while the process keeps running, what
  `CloseReason`/`detail` does the forwarder stamp? MVP: treat as a local close
  with a best-effort reason; refine alongside the edge-client's
  malformed-frame policy (edge-client §13).
- **Flush bound on action exit** — how long, if at all, to let the terminal
  `Close` flush before returning the action result (§5.1); part of the best-effort
  coupling question (§8).
