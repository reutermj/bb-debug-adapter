# DAP Relay — Worker-Side Forwarder

> Status: **Draft for refinement.** This is the §7.4 follow-up. It specifies the
> worker-side forwarder, which bridges a debuggable action's DAP server to the
> relay. It builds directly on the **edge-client** (`edge-client.md`) as
> `side = FORWARDER` and on the §5.3 decisions. Section references like "§5.3"
> point at `high-level-design.md`; "edge-client §X" points at `edge-client.md`;
> "protocol §X" at `relay-protocol.md`.

## 1. Role and placement

The forwarder lives **inside `bb_worker`**, in the per-thread executor stack,
running **concurrently with the blocking `Runner.Run`** call (§5.3). For a
debuggable action it dials the action's local DAP port and hands that connection
to an embedded edge-client (`side = FORWARDER`), which does all the protocol
work. The forwarder itself only adds the worker-specific glue: deciding
debuggability, port/env handling, the `Runner.Run` goroutine, and terminating the
action when the debugger leaves.

The runner (`bb_runner`) is **untouched** (§5.3): the forwarder dials the action
over loopback, which is reachable because there is no network-namespace isolation
by default.

## 2. Triggering: debuggability (§5.5)

An action is debuggable **iff** its `Command.environment_variables` contains
`BB_DEBUG_SESSION_ID` (§5.1, §5.5). The executor reads this from the `Command` it
already loads to run the action. If absent, the forwarder does nothing and
execution is entirely unchanged. If present, its value is the `session_key`.

## 3. Port discovery and env injection (§5.3)

For a debuggable action the executor:

1. Computes `BB_DEBUG_PORT = debug_port_range_start + threadID`, where `threadID ∈
   [0, Concurrency)` is the stable per-thread id the worker already tracks
   (`workerID["thread"]`). The allocation is deterministic and collision-free
   across a worker's slots — no free-port probing, no TOCTOU race (§5.3).
2. **Injects `BB_DEBUG_PORT`** into the action's environment (the executor already
   builds this map; the injection is the one real code change, and it passes
   through `chroot` fine, §5.3).

**The action's contract.** A debuggable action is responsible for starting a DAP
server listening on `127.0.0.1:$BB_DEBUG_PORT`. That is the user's concern (a
launch-wrapper, etc.), not the forwarder's — the forwarder only dials that port.
Because one thread runs one action at a time, the port is never concurrently
reused; sequential actions on a thread may hit `TIME_WAIT` on rebind, mitigated by
`SO_REUSEADDR` on the DAP server or a brief delay (§5.3).

## 4. Lifecycle

```
 Runner.Run(ctx) ───────────────────────────────────────────── blocks ──▶ returns
        │ (concurrent)
        ▼
 forwarder goroutine:
   retry-dial 127.0.0.1:BB_DEBUG_PORT ──connected──▶ run edge-client(side=FORWARDER,
        │  (until connected or action exits)            session_key, action conn)
        │                                                       │
        └── action exits before connect ──▶ give up            ▼ terminal outcome
                                                         (§5 termination)
```

1. **Start** (debuggable only): inject `BB_DEBUG_PORT` (§3) and spawn the
   forwarder goroutine alongside `Runner.Run`.
2. **Retry-dial** `127.0.0.1:BB_DEBUG_PORT` with backoff until the DAP server is
   listening, or give up when the action exits (`Runner.Run` returned). Readiness
   is not otherwise observable — the DAP server comes up sometime after the action
   starts (§5.3). A proxy that connected first simply has its messages buffered at
   the relay until the forwarder attaches (store-and-forward, §5.4).
3. **Bridge**: hand the dialed connection to the edge-client (edge-client §11) with
   `side = FORWARDER`, `session_key` from `BB_DEBUG_SESSION_ID`, the worker's relay
   access (§6), and a close-reason source (§5.1). The edge-client runs the
   protocol until a terminal outcome.

## 5. Termination

The forwarder ends when either the action exits or the debugger leaves; these are
the two drivers, and they meet in the middle.

### 5.1 Action exits first (normal / debugged-to-completion)

`Runner.Run` returns (action completed, or was killed). The worker tears down the
forwarder. The action — and thus its DAP server — is gone, so this is a **local
close** (edge-client §9.1): the edge-client emits a terminal
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

If the action exits before its DAP server ever accepts a connection (no DAP
server, early crash, wrong port), the forwarder gives up dialing. Any proxy
waiting on the relay never gets a peer and is eventually reaped by the relay's
orphan timeout (protocol §8). The action's own result is unaffected.

## 6. Reaching the relay

The forwarder dials **`bb_dap_relay` directly**, not through the `bb_storage`
frontend (§5.2 permits this for internal farm components). The worker holds one
relay gRPC client (configured per §7), and each forwarder opens an `Attach` stream
on it via the edge-client. This reuses the worker's existing pattern of holding
outbound gRPC clients to the farm.

## 7. Configuration

New worker configuration:

- **`debug_port_range_start`** — base of the per-thread debug port range
  `[start, start + Concurrency)`. Its presence enables worker-side debug
  forwarding; absent ⇒ the worker ignores `BB_DEBUG_SESSION_ID` entirely.
- **Relay endpoint + credentials** — gRPC client config for dialing
  `bb_dap_relay` (endpoint, TLS, keepalive, edge-client §10).

**Startup validation** (§5.3): the worker checks at startup that
`[start, start + Concurrency)` does not overlap another runner/worker's range on
the same host, and does not intersect the OS ephemeral range (Linux
~`32768–60999`); fail or warn loudly otherwise.

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
  per-thread executor to decide debuggability (§2).
- **Inject `BB_DEBUG_PORT`** into the action's env map in that same per-thread
  executor (§3) — the only change to the action's environment.
- **Wrap `Runner.Run`** (the `runner.Run` call in `local_build_executor.go`) so the forwarder
  goroutine runs concurrently and is torn down when `Run` returns, and so a
  `PeerClosed` outcome can cancel the run (§5.2).
- **Construct the relay client** once per worker from config (§7), shared across
  threads.

Everything protocol-facing (sequencing, acks, retain/replay, dedupe, reconnect,
framing, the terminal `Close`) is the **edge-client's**, not duplicated here.

## 10. Open questions

- **Best-effort coupling (open)** — see §8; the action-vs-debug-failure
  coupling policy is intentionally unresolved.
- **Forwarder close reason when the DAP socket dies but the action lives** — if
  the action's DAP server closes/garbles while the process keeps running, what
  `CloseReason`/`detail` does the forwarder stamp? MVP: treat as a local close
  with a best-effort reason; refine alongside the edge-client's
  malformed-frame policy (edge-client §13).
- **Flush bound on action exit** — how long, if at all, to let the terminal
  `Close` flush before returning the action result (§5.1); part of the best-effort
  coupling question (§8).
