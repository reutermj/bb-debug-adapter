# Buildbarn Debug Adapter Relay — High-Level Design

> Status: **Draft / exploratory.** This document captures the high-level goals
> and how the new service fits into the existing Buildbarn architecture. The
> concrete protocol, proto definitions, and component boundaries are
> intentionally left for follow-up documents once the high-level shape is
> agreed.

## 1. Problem statement

When an action runs on a Buildbarn remote worker, a developer driving the build
from Bazel has no way to attach an interactive debugger to that remotely-running
process. We want to let a developer use a standard
[Debug Adapter Protocol](https://microsoft.github.io/debug-adapter-protocol/)
(DAP) client — e.g. the debugger built into VS Code — to debug a process that is
actually executing inside a `bb_runner` on the build farm, as if it were running
locally.

We deliberately scope this down:

- We support **DAP adapters that speak DAP over stdin/stdout** — the default,
  native DAP transport (the mode in which a DAP client normally launches an
  adapter as a child process). The debuggable action *is* (or launches) such an
  adapter. Adapters that **only** listen on a TCP port and cannot speak stdio are
  out of scope for the first iteration; a user-supplied stdio↔socket wrapper can
  bridge them if needed.
- The new service we are designing is intentionally **DAP-agnostic**: it is a
  rendezvous buffer that holds DAP messages flowing in each direction between the
  developer and the remote debug-adapter server. It implements reliable,
  resumable message transport (sequencing, acks, replay) but never parses or
  interprets DAP — "dumb about DAP," not logic-free.

## 2. Background: the existing Buildbarn architecture

Understanding where to hook in requires understanding the request path. Two
repositories are relevant here:

- **`bb-storage`** — provides `bb_storage`, the gRPC frontend that Bazel talks
  to. It implements the Remote Execution v2 (REv2) APIs: the Content
  Addressable Storage (CAS), the Action Cache, and the Execution service. It
  routes execution requests to the scheduler.
- **`bb-remote-execution`** — provides `bb_scheduler`, `bb_worker`, and
  `bb_runner`.

### 2.1 Request path

```
                  REv2 gRPC                          internal gRPC
   ┌────────┐  (CAS / AC / Exec)    ┌──────────────┐   (Execute)   ┌───────────────┐
   │ Bazel  │ ───────────────────▶ │  bb_storage  │ ────────────▶ │  bb_scheduler │
   │(client)│ ◀─────────────────── │  (frontend)  │ ◀──────────── │   (queue)     │
   └────────┘                       └──────────────┘               └───────┬───────┘
                                                                           │ OperationQueue
                                                                           │ .Synchronize()
                                                                           ▼
                                                                   ┌───────────────┐
                                                                   │   bb_worker   │
                                                                   │ (orchestrates │
                                                                   │  execution)   │
                                                                   └───────┬───────┘
                                                                           │ Runner.Run()
                                                                           │ (gRPC over a
                                                                           │  local socket)
                                                                           ▼
                                                                   ┌───────────────┐
                                                                   │   bb_runner   │
                                                                   │ (spawns the   │
                                                                   │  action proc) │
                                                                   └───────────────┘
```

Key points relevant to this design:

1. **Action digests are a shared identity.** The Bazel client constructs an
   REv2 `Action`, uploads it to the CAS, and calls `Execute` with the resulting
   `action_digest`. That same digest is propagated to the worker via the
   scheduler (`DesiredState.Executing.action_digest`). Both the client and the
   worker therefore independently know the action digest — making it a natural
   rendezvous key, though not necessarily the only option (see §5.1).

2. **`Runner.Run` is synchronous and blocking.** `bb_worker` calls
   `Runner.Run` (`pkg/proto/runner/runner.proto`) and the call only returns
   once the action process has exited. See the `runner.Run` call in
   `bb-remote-execution/pkg/builder/local_build_executor.go`. There is no
   existing interactive side channel during execution. Any debug forwarding has
   to happen *concurrently* with an in-flight `Run` call.

3. **`bb_worker` and `bb_runner` share a host and filesystem** and communicate
   over a local (typically Unix-domain) gRPC socket. The runner spawns the
   action with a working directory inside the build directory.

4. **Workers are not directly addressable by clients.** They sit behind the
   scheduler, are ephemeral, and generally have no inbound connectivity from
   developer machines. Therefore the developer and the worker cannot connect to
   each other directly; they need a mutually-reachable meeting point — exactly
   the role of the new service.

## 3. High-level goals

1. A developer runs a **local DAP proxy server** on their own machine. Their DAP
   client (VS Code, etc.) connects to it exactly as if it were a normal,
   locally-hosted debug adapter. The proxy hides all of the remoting.

2. The **remote debug adapter** runs as (or is launched by) the action inside
   `bb_runner`, speaking DAP over its **stdin/stdout** — the native DAP transport.

3. The **`bb_worker`/`bb_runner` side forwards** the action's stdio: the runner
   wires the action's stdin/stdout to a Unix-domain socket in the shared build
   directory (teeing stdout so the normal action log is still captured), and the
   worker bridges those bytes to/from the new relay service.

4. The **new relay service is a per-session bidirectional buffer**. It holds two
   message streams per debug session — developer→server and server→developer —
   and lets each side read what the other has written. It is **DAP-agnostic
   transport**: it implements reliable, resumable delivery but never parses or
   interprets DAP.

## 4. Proposed integration

We introduce one new component plus two thin adapters at the edges.

The developer's proxy reaches the relay through the existing `bb_storage`
frontend, which routes the `DebugAdapterRelay` service and forwards its stream to
a separate single-node `bb_dap_relay` backend (§5.2). The worker forwarder is just
another gRPC client of the same service; being an internal farm component, it
dials `bb_dap_relay` directly rather than through the frontend (§5.2).

```
 Developer's machine            Build farm
 ┌──────────────────────┐
 │  DAP client (VS Code) │
 │          │ stdio      │  (client launches the proxy as a stdio adapter)
 │          ▼            │       ┌──────────────┐ fwd  ┌────────────────────┐
 │  Local DAP proxy      │ gRPC  │ bb_storage   │─────▶│  bb_dap_relay      │
 │  adapter (NEW)        │ ────▶ │ frontend     │◀─────│  (NEW, "the buffer")│
 │                       │ ◀──── │ (demux only) │      │  per-session, two   │
 └──────────────────────┘       └──────────────┘      │  directional logs   │
                                                       └─────────┬──────────┘
                                       gRPC (direct from the     ▲
                                       worker, not via frontend) │
                                      ┌──────────────────────┐   │
                                      │  bb_worker            │───┘
                                      │  forwarder (NEW)      │
                                      │          ▲            │
                                      │          │ build-dir  │
                                      │          │ UDS        │
                                      │  ┌───────┴─────────┐  │
                                      │  │  bb_runner      │  │
                                      │  │  wires action   │  │
                                      │  │  stdin/stdout   │  │
                                      │  │  to the UDS     │  │
                                      │  │      │ stdio     │  │
                                      │  │      ▼           │  │
                                      │  │  Remote DAP      │  │
                                      │  │  adapter (the    │  │
                                      │  │  action)         │  │
                                      │  └─────────────────┘  │
                                      └──────────────────────┘
```

### 4.1 New / changed components

| Component | Where it runs | Responsibility |
|---|---|---|
| **DAP Relay service** (`bb_dap_relay`, new) | A separate farm backend, fronted by the `bb_storage` frontend that demuxes/forwards to it (§5.2); single node for the MVP | Per-session bidirectional message buffer. Implements reliable, resumable, sequenced delivery between the two sides, but is **DAP-agnostic** (never parses payloads). |
| **Local DAP proxy** (new) | Developer's machine | A stdio DAP adapter the DAP client launches. Bridges its own stdin/stdout to the relay service for a given session key. |
| **Worker/runner forwarder** (new) | The relay-facing half in `bb_worker` (concurrent with `Runner.Run`); a minimal stdio-wiring step in `bb_runner` | The runner wires the action's stdin/stdout to a build-directory Unix-domain socket (teeing stdout to the normal log). The worker accepts that socket and bridges the bytes to the relay service for the matching session key. |

### 4.2 Data flow for one debug session

0. The developer generates a session key (UUID) and starts the build with it set
   as an action environment variable (`BB_DEBUG_SESSION_ID`, via `--action_env`),
   passing the same key to the local proxy (see §5.1).
1. The worker reads `BB_DEBUG_SESSION_ID` from the action's `Command`. Because the
   action is debuggable, the worker creates a Unix-domain socket in the build
   directory, passes its path to the runner, and the runner wires the action's
   stdin/stdout to that socket (teeing stdout to the normal action log) before
   spawning the action. The action runs a DAP adapter that speaks DAP over stdio.
2. The worker's forwarder accepts the build-directory socket and registers with
   the relay under that session key.
3. The developer's DAP client launches the local proxy (a stdio adapter); the
   proxy connects to the relay under the same session key.
4. DAP messages flow developer→proxy→relay→forwarder→DAP adapter and back, with
   the relay simply buffering and handing messages across.

## 5. Design decisions and open questions

Some of the decisions the high-level shape exposes have been made; others are
still open. Each subsection is marked **Decided** or **Open**.

### 5.1 Session identity / rendezvous key — **Decided (revisit later)**

Both ends must independently arrive at the same session key. The constraints
are that the key be (a) unique per debug session, (b) discoverable by both the
developer's proxy and the worker-side forwarder, and (c) usable as an
authorization scope.

**Decision: carry a developer-generated session key as an action environment
variable** (e.g. `BB_DEBUG_SESSION_ID=<uuid>`, set via Bazel's `--action_env`).
The developer generates the UUID, passes it to Bazel, and passes the same UUID
to the local proxy on its CLI. The worker reads it straight out of the
`Command.environment_variables` it already loads to run the action
(`bb-remote-execution/pkg/builder/local_build_executor.go`).

Why an environment variable and not the other candidates:

- **Not a platform / exec property.** The scheduler builds its platform-queue
  key from the *entire* REv2 `Platform` message
  (`bb-remote-execution/pkg/scheduler/platform/key.go`), and action-to-worker
  matching is an **exact** match on that platform string
  (`platform/trie.go` — only the instance name is prefix-matched, not the
  properties). A per-session-unique value in `Platform` therefore produces a
  brand-new platform string that **no worker advertises**, so the action routes
  to an empty queue and never schedules (the platform-queue lookup in
  `in_memory_build_queue.go` fails its longest-prefix match).
  `--remote_default_exec_properties=<unique>` would break execution outright.
- **Not the action digest.** The worker knows it, but it is content-addressed
  (not unique per session, not secret) and the developer cannot easily single
  out which of a build's many action digests to attach to.
- **Not `RequestMetadata.tool_invocation_id`.** It is a natural per-invocation
  id, but it does **not currently reach the worker**: `DesiredState.Executing`
  (`pkg/proto/remoteworker/remoteworker.proto`) carries the `action_digest` and
  `Action`, not the `RequestMetadata` the scheduler parses for queue fairness.
  Using it would require new scheduler→worker plumbing (e.g. via
  `auxiliary_metadata`).

Properties of the env-var approach:

- It does **not** affect platform-queue selection (env vars are part of the
  `Command`, not the `Platform`).
- It **does** change the action digest, causing a cache miss — which is
  desirable here: a debug run should genuinely execute rather than return a
  cached result.

**Why this is not a one-way door:** the session-key *carrier* is an
implementation detail of how the two ends rendezvous. Switching later to
`tool_invocation_id` (with scheduler→worker plumbing) or a relay-issued ticket
would change how the key is produced and delivered, but not the relay protocol
or the overall data flow. We start with the env var for simplicity and revisit
if a less manual / more automatic identity scheme is warranted.

### 5.2 How the developer's traffic reaches the relay — **Decided: config-driven frontend relay to a separate `bb_dap_relay`**

Direction: the developer's proxy connects only to the **existing `bb_storage`
frontend** it already uses, and the frontend forwards the `DebugAdapterRelay`
stream to a separate `bb_dap_relay` backend. The proxy thus reuses the frontend's
endpoint, TLS, and (eventually) auth, and never needs a new port or transport
stack. The worker-side forwarder is just another gRPC *client* of the relay
(dialing `bb_dap_relay` directly, §4).

**This needs no `bb_storage` code change — it is config-driven.** `bb_storage`
already ships a **generic, transparent, bidirectional stream relay** that routes
by gRPC **service name** to a backend, configured purely through the
`GrpcServers` config — no recompilation, no forked `main.go`. The pieces:

- `ServerRelayConfiguration` (`bb-storage/pkg/proto/configuration/grpc/grpc.proto`,
  the `relays` field of a gRPC server) takes `{ endpoint, services: [...] }`.
- For each listed service, the server installs `NewForwardingStreamHandler`
  (`bb-storage/pkg/grpc/forwarding_stream_handler.go`) — a generic handler that
  opens a **bidirectional** backend stream (`ServerStreams` *and* `ClientStreams`
  true), pumps both directions, and treats every message as opaque
  (`emptypb.Empty`), so it never parses the payload.
- These are wired by service name via `NewRoutingStreamHandler`
  (`routing_stream_handler.go`) registered as the server's gRPC
  `UnknownServiceHandler` (`pkg/grpc/server.go`), so any service **not** statically
  registered on the frontend falls through to the relay route.

So fronting the relay is a config entry on the frontend's gRPC server, e.g.:

```jsonnet
relays: [{
  endpoint: { address: 'bb_dap_relay:…', /* TLS, keepalive, … */ },
  services: ['buildbarn.daprelay.v1.DebugAdapterRelay'],
}]
```

Because `DebugAdapterRelay` is not statically registered on the frontend, the
unknown-service handler catches it and transparently forwards the `Attach` bidi
stream to `bb_dap_relay`. (This corrects an earlier draft of this section, which
claimed the frontend's service list was a compile-time closure requiring a
forked `main.go`; the `relays` facility post-dates that reasoning.)

Three things to **validate** when we build against this facility, none expected to
be blockers (tracked in relay-protocol §10):

- **Payload fidelity.** The forwarder round-trips each message through
  `emptypb.Empty`; protobuf stores unrecognized fields as raw bytes and re-emits
  them verbatim, and the load-bearing DAP bytes live in `Frame.payload` (a `bytes`
  field), which round-trips losslessly — confirm this preserves payloads exactly,
  since the end-to-end "verbatim bytes" guarantee (§5.4) rides on it.
- **Half-open propagation.** The relay flips to retain-for-replay on a stream
  error (§5.4). For the proxy leg the proxy's keepalive terminates at the
  *frontend*, so the leg whose death must actually be detected is
  **proxy↔frontend**, governed by the **frontend's gRPC server-side keepalive
  enforcement plus the proxy's client keepalive** — not only the frontend↔relay
  leg. Once the frontend's forwarding stream handler sees the incoming
  (proxy-side) stream error it cancels the backend stream
  (`forwarding_stream_handler.go` cancels its derived context on an incoming
  `RecvMsg` error — confirmed), tearing down frontend↔relay so the relay flips to
  retain-for-replay. So: configure keepalive on **both** legs — the frontend's
  server keepalive toward the proxy (to notice the dead proxy promptly) and the
  frontend↔relay client leg.
- **Connection-age churn.** If the frontend sets `MaxConnectionAge`, it will
  periodically recycle the proxy's stream; harmless (the protocol resumes via
  `Resume`, §5.4) but means the proxy sees policy-driven reconnects, not just
  network blips.

**Buffer location — Decided: a separate `bb_dap_relay` service.** The per-session
buffer lives in its own backend service, **not** in the frontend process. The
`bb_storage` frontend's only role is to be the single gRPC endpoint that routes
the `DebugAdapterRelay` service (by service name) and **transparently forwards**
the stream to `bb_dap_relay` — analogous to how the frontend can relay other
services. This keeps the buffer's memory and lifecycle out of the
latency-sensitive storage frontend.

**Multi-replica — Decided (MVP): a single `bb_dap_relay` node.** There is exactly
**one** `bb_dap_relay` instance that all frontend shards forward to, so all state
for a session naturally lives in one place. We do **not** solve horizontal
scaling (session affinity / shared state across multiple relay replicas) for the
MVP; a single node sidesteps it entirely. Scaling out is a later, backward-
compatible concern.

### 5.3 Concurrency with the blocking `Run` call — **Decided (stdio over a build-dir socket)**
The forwarder must run while `Runner.Run` is in flight.

**Transport — Decided: the action's stdio, bridged over a build-directory
Unix-domain socket.** DAP's native transport is stdin/stdout: a DAP client
normally launches an adapter as a child and speaks DAP over its stdio. We use
that same transport remotely. The mechanism rests on a fact about the existing
code: **`bb_runner` is the sole owner of the action's stdio fds.** It sets
`cmd.Stdin`/`cmd.Stdout`/`cmd.Stderr` in `pkg/runner/local_runner.go` (today:
stdin unset → `/dev/null`; stdout/stderr → log files named by path strings the
worker passes in `RunRequest`). `bb_worker` only hands the runner **path
strings**; it never sees the live fds. So the byte path necessarily goes through
the runner, but only minimally:

- The worker (per-thread executor, concurrent with `runner.Run`) creates a
  **Unix-domain socket in the shared build directory** and passes its path to the
  runner in a new, optional `RunRequest` field — set **only** when the action is
  debuggable (`BB_DEBUG_SESSION_ID` present). `bb_worker` and `bb_runner` already
  share the build-directory filesystem and already communicate by passing paths
  into it, so this fits the existing contract exactly.
- The runner, when that path is set, **dials** the socket and wires the action's
  stdio to it: `cmd.Stdin = conn` (the client→server bytes), and
  `cmd.Stdout = io.MultiWriter(stdoutFile, conn)` — a **tee** so the action's
  stdout is *both* captured to the normal `ActionResult` log *and* mirrored to the
  debug channel (server→client bytes). `cmd.Stderr` is **untouched** — it stays a
  normal log channel (adapters typically write diagnostics there).
- The worker **accepts** the runner's connection and hands the resulting
  `net.Conn` to the edge-client (`side = FORWARDER`); the edge-client does all the
  protocol work, identically to the port-free past — it just bridges this conn
  instead of a dialed one.

Why this is small and clean:

- **No port machinery at all.** No port assignment, no per-thread range, no range
  coordination across runners/workers, no ephemeral-range avoidance, no
  `TIME_WAIT`-on-rebind, no `BB_DEBUG_PORT` to compute, inject, or communicate to
  the adapter's CLI. The action just runs a stdio adapter (the common default,
  e.g. `lldb-dap` with no extra flags).
- **Deterministic readiness — no retry-dial.** The worker creates the listener
  *before* calling `Run`; the runner dials it as part of setting up the action's
  stdio. The forwarder accepts exactly once. There is no "is the DAP server
  listening yet?" race (the action's stdout exists from `exec`); a proxy that
  connects first simply has its messages buffered at the relay (store-and-forward,
  §5.4) until the forwarder attaches.
- **Robust under sandboxing.** The action's stdio is delivered as **inherited
  fds**, which cross `chroot` and network-namespace boundaries unchanged.
  Deployments that add network sandboxing (gVisor, bubblewrap with a net ns) —
  which would have broken a loopback-port dial — work here with no special
  handling. fds cross namespaces; ports do not.

**Forwarder placement — Decided: relay-facing half in `bb_worker`, minimal
stdio-wiring in `bb_runner`.** The relay-protocol-speaking forwarder (the
edge-client) lives in `bb_worker`: it already holds outbound gRPC clients to the
farm (so reaching the relay is just another client) and owns the goroutine around
the blocking `runner.Run` call (so it can run concurrently and tear down on
completion). `bb_runner` gains only a tiny, **network-egress-free** step — dialing
a *local socket file in the build directory* and wiring three fds, exactly the
kind of local-path work it already does for stdout/stderr. The old objection to
involving the runner (*"don't give the intentionally-dumb, less-privileged runner
network egress"*) does **not** apply: the runner never touches the network; the
relay client stays in the worker. So we get DAP's native stdio transport while
keeping the forwarder — and all egress — in the worker.

Considerations / operator notes:
- **stdout must carry pure DAP framing.** Because the action's stdout *is* the
  server→client DAP channel, the action must **be** the DAP adapter (or the
  adapter must own stdout and emit the debuggee's program output as DAP `output`
  events). A stray write to stdout from a wrapper, the shell, or the debuggee
  would interleave into the frame stream and corrupt it. This is the action's
  contract (it replaces the old "bind a DAP server on a port" contract). `stderr`
  is free for diagnostics.
- **The build log will contain DAP bytes during a debug run.** Because stdout is
  teed, the `ActionResult` stdout shipped back to Bazel contains the
  (LSP-framed) server→client DAP traffic. This is expected and benign — ugly but a
  faithful recording of that half of the session; the developer's real view is the
  debugger UI, where program output arrives as DAP `output` events.
- **Tee backpressure.** `io.MultiWriter(stdoutFile, conn)` blocks on its slower
  sink, so a stalled relay could backpressure the action's stdout. At interactive
  debug volume this is negligible; if it matters, the runner can buffer the
  debug-channel write asynchronously (DAP bytes must never be dropped, so the
  buffer grows or the write blocks — it cannot discard). Left as an operator/impl
  note, not an MVP blocker.
- **The real code changes** are: a new optional `RunRequest` field for the
  socket path; the conditional stdio-wiring block in `local_runner.go`; and the
  worker-side listener-create / accept / hand-off that replaces the old port
  logic. The relay, protocol, edge-client, and proxy are unchanged by the choice
  of transport (see §7).

### 5.4 Lifecycle and buffering semantics — **Decided (store-and-forward), with open edges**

**Decision: the relay is a store-and-forward buffer, message-oriented, with
per-direction sequence numbers and replay on reconnect.**

Model:
- A session has **two independent directions** (client→server and
  server→client). Each direction is an **ordered log of discrete whole DAP
  messages** tagged with a per-direction sequence number.
- The relay **retains** messages until the peer acknowledges them and
  **replays** unacknowledged messages when a side reconnects.
- **Neither side has to be present for the other to make progress.** A side may
  connect before its peer (the developer attaches before the action's DAP server
  is up, or vice versa); its messages queue and are delivered when the peer
  appears. There is no special rendezvous handshake in the relay.

Why this gives disconnect tolerance: the goal is that a brief disconnect that
resolves quickly does not perturb the local DAP client or the remote DAP server.
That is achieved by two things together:
1. **The edges keep their local connections open across a relay reconnect.** The
   forwarder's connection to the action's stdio and the proxy's stdio connection
   to the DAP client stay up while the gRPC stream to the relay drops and
   re-establishes. Neither DAP endpoint observes a disconnect — only a pause.
2. **Sequence numbers + replay** ensure the reconnecting side resumes exactly
   where it left off, with no lost or duplicated messages.

**Message framing.** Messages are framed at the **edges, not in the relay**. DAP
uses LSP-style framing — a `Content-Length: <n>\r\n\r\n` header followed by `n`
bytes of JSON — which is trivial to parse (read the header, read `n` bytes; no
JSON parsing needed). The proxy and forwarder each read a *complete* DAP message
off their local socket — header included — and hand it to the relay **verbatim**
as one discrete payload; in the reverse direction they write that payload back to
the local socket **unchanged** (no re-framing, no reconstruction). The relay deals
only in **discrete opaque payloads** — message-oriented (so a reconnect never
resumes mid-message) while remaining fully DAP-agnostic, and the original bytes
are preserved exactly end to end.

**Handshake / who waits.** DAP's handshake is client-driven: the client sends
`initialize`, then `launch`/`attach`, and the remote side (the DAP *server*)
only responds. Because the buffer holds the client's messages until the
executor-side forwarder drains them, the normal DAP path "just works" with no
custom sequencing. *Caveat:* DAP clients impose a timeout on the
`initialize`/`attach` request; if the executor side is slow to come up
(scheduling queue + input fetch before the DAP server is listening), the client
may time out. This is a client-config / proxy-UX concern (attach timeout,
"connecting…" affordance), not a relay design issue.

Implied protocol shape: a **bidirectional streaming RPC** carries each edge's
connection; the first message declares `{session_key, side}`; subsequent messages
are `{seq, payload}` (one whole DAP message) plus acknowledgements; the relay
keeps a per-direction sequence-numbered log, forwards to the connected peer, and
replays unacked messages on reconnect.

*Not a hard requirement:* whether both edges call **one** RPC (distinguishing
their role via the `side` field) or whether the client and worker get **separate
per-role RPCs** is a deliberately-open, reversible API-surface choice — it
changes a method name and an enum field, not the protocol or data flow. What
*is* load-bearing is that both roles share the **same stream message grammar**
(so a single relay-client library serves both the proxy and the forwarder). For
the MVP we default to **one shared RPC with a `side` field** because the two
roles are symmetric today; we revisit only if they diverge (e.g. per-role auth
under §5.6, or role-specific metadata). We equally do **not** mandate multiple
RPCs.

**Message GC / buffer bounds — Decided.** The per-session buffer is
flexibly sized and retains only the **in-flight (sent-but-unacked) window** per
direction; normal reclamation is driven by an **application-level cumulative
ack**, not by any gRPC transport signal.

- **No transport-level "delivered" signal exists.** `stream.Send()` returning
  nil means the message reached the local HTTP/2 transport, **not** that the
  peer's application received it; HTTP/2 flow control is backpressure, not an
  app-visible delivery signal. Neither is safe as a GC trigger.
- **GC trigger = receiving-edge ack.** Each message carries a per-direction
  `seq`. The receiving edge acks `seq N` once it has written that message to its
  **local socket** (the DAP client, or the action's stdio — "the right place").
  Cumulative: "I have through N." The relay then drops everything `≤ N` in that
  direction. Under healthy flow the retained window is a few messages; it grows
  only when a peer is slow or absent.
- **Resume point comes from the receiver, not the relay's stored ack.** On
  (re)connect the receiving side announces its **high-water mark** (highest
  contiguous `seq` it has durably written to its local socket) as the first
  message, and the
  relay replays from `high_water + 1`. The relay's own stored last-ack must *not*
  drive resume: it can lag the receiver (acks in flight when the stream dropped),
  which would cause needless duplicate replays, and it can never be more current
  than the receiver. As a **defensive backstop**, the receiver also discards any
  delivered message with `seq ≤ high_water` — this keeps things correct even if
  the relay over-replays or **restarts and loses its ack bookkeeping**.
- **This applies to both hops.** There are two reliable gRPC hops around the
  relay (`forwarder → relay` and `relay → proxy`). The receiving end of each hop
  owns the resume point and announces its high-water on reconnect; the sender
  resumes from there and the receiver dedupes. Consequently each sender (the
  forwarder, and the relay) must retain its own sent-but-unconfirmed messages
  until its downstream acks receipt — the "`Send()` is not delivery" gotcha
  applies on every hop, not just at the edges.
- **What gRPC contributes:** the bidirectional stream *carries* the acks;
  HTTP/2 flow control provides **best-effort** backpressure on a slow *consumer*
  (when the relay's own `Send` to that consumer blocks, it naturally stops
  forwarding that direction); stream EOF/errors and keepalive PINGs let the relay
  detect a disconnect and switch from GC to *retain-for-replay*. **Flow control is
  not the authoritative memory bound** — because each edge's data and its acks
  ride the *same* bidi stream, the relay cannot pause a producer without stalling
  the reverse direction's acks, so it keeps reading both streams and relies on the
  per-session **byte cap (below) as the real bound** (see relay-server §6 for the
  full reasoning).
- **Ack boundary rationale:** "written to the local socket" is the strictest
  signal available — DAP has no transport-level ack, and if an edge dies after
  writing, its local connection dies too, so the peer DAP endpoint sees a
  disconnect and the session is finished anyway.
- **Other GC levers (kept):** *piggybacked acks* (carry the ack `seq` on data
  flowing the other way — an optimization); *whole-session teardown GC* (drop the
  entire buffer on session end); and a **hard-cap safety valve** — if the unacked
  window exceeds a **hard-coded byte cap**, the session is **aborted** (evicting
  an unacked message would corrupt the DAP session, so the cap must abort, not
  silently drop). See the "Buffer cap" decision below for the punt to a simple
  hard-coded size (no age dimension, no tuning) in the MVP.

**Session materialization — Decided: lazy first-touch, symmetric, implicit.**
A session springs into existence the moment the **first** edge connects with a
given `session_key`; the relay allocates the per-session state (the two
directional logs) as a side effect of that first `Hello{session_key, side}`
message. There is no `CreateSession` RPC and no designated "creator" side —
either the proxy or the forwarder may arrive first, and the second edge simply
attaches to the already-materialized session. This is the natural realization of
the store-and-forward semantics above ("a side may connect before its peer … its
messages queue and are delivered when the peer appears; there is no special
rendezvous handshake"). We knowingly accept that, with auth deferred (§5.6), any
connection bearing any UUID allocates state — abuse mitigation is out of scope
for the MVP and the unguessable key is the only gate.

**Session teardown — Decided: explicit terminal-close marker + drain-then-drop,
with timeouts as safety nets.** Reclaiming a session must not break the
disconnect-tolerance guarantee above, so the design rests on one distinction and
one rule.

*The distinction — a dropped gRPC stream is not a teardown.* A stream drop is
ambiguous (did the edge crash, or is it mid-reconnect?), so it can never by
itself end a session. The relay distinguishes:
- **Transient relay-hop disconnect** — the edge's *local* connection (to the DAP
  client or the action's stdio) is still alive and it will reconnect → **retain
  and await replay**.
- **Terminal end** — the edge's *local* connection has closed (the DAP client
  quit, or the action/DAP adapter exited) → the DAP session is genuinely over →
  **reap**.

To make this observable rather than guessed, an edge sends an **explicit
terminal-close marker** when its local socket closes — distinct from merely
letting the gRPC stream drop. The protocol (§7.3) must carry this marker.

*The rule — teardown is drain-then-drop, not an immediate purge.* The terminal
marker is placed in the log **after the last message** in that direction; the
relay keeps delivering and getting acks for the messages still buffered for the
peer, then reaps. This protects the most important final messages — the DAP
`terminated`/`exited` events — and is consistent with the §5.4 principle that
evicting an unacked message corrupts the session. Action-exit ends both
directions asymmetrically: new client→server messages are refused/discarded
(the DAP server is gone), while buffered server→client messages must still drain
to the proxy.

*Authority and propagation.*
- **Action exit is the authoritative terminal signal.** When `Runner.Run`
  returns, the worker tears down the forwarder (§5.3); the forwarder is the right
  emitter of the server-side terminal marker (optionally with a reason:
  exited / killed / timed out). The thing being debugged is gone, so the session
  has no reason to live.
- **Client-gone reaps the session and (MVP) terminates the action.** When the
  proxy's terminal `Close` reaches the forwarder, the relay reaps the session
  state *and* the worker terminates the action (see the "Terminate the action on
  debug-session-end" decision below). This is a deliberate coupling: a debug run
  exists to be debugged, so when the debugger leaves we end the run rather than
  letting it continue unattended. Letting the action keep running after a detach
  is an attach-style feature punted past the MVP.
- On reap, the relay closes the surviving peer's stream with a status so its edge
  stops reconnecting and propagates the end downward; teardown is **idempotent**
  (both edges may signal end concurrently).

*Timeouts (safety nets only — named here, values still open):*
- **Reconnect grace window** — how long to retain a session after a stream drops
  with *no* terminal marker, before giving up on replay.
- **Orphan timeout** — materialized, but the second edge never connects.
- **Idle timeout** — fires on a session whose edges are **disconnected** (no live
  gRPC stream / no keepalive heartbeat), not on a healthy-but-quiet one. A
  developer paused at a breakpoint with both streams up and heartbeating is a
  perfectly live session and must **not** be reaped, however long the quiet
  lasts; idleness is measured by absence of connections, never by absence of DAP
  traffic.

**Buffer cap — Decided (punted): hard-coded max bytes, abort on exceed.** The
relay caps the retained (unacked) bytes per session; if the buffer exceeds that
cap, the session is **aborted** (consistent with "evicting an unacked message
would corrupt the session, so the cap must abort, not silently drop"). For the
MVP the cap is a **hard-coded constant** — no config knob, no separate age cap,
and no attempt to distinguish "pure backpressure up to the cap." We deliberately
do **not** tune this now: debug traffic is low-volume and interactive, so the cap
is a pathological-case safety valve that should never fire in the common MVP
path. This is **not a one-way door** — making the cap configurable, adding an age
dimension, or unifying it with the teardown reconnect-grace window are all
backward-compatible refinements that change a number/policy, not the protocol or
data flow.

**Remaining lifecycle details — Decided.**
- **Teardown timeout values — configurable.** The reconnect-grace, orphan, and
  idle timeouts are exposed in the normal Buildbarn configuration files (jsonnet),
  not hard-coded. Sensible defaults ship; operators tune them.
- **Peer-facing end semantics — do not synthesize.** The relay and proxy do
  **not** fabricate any DAP `terminated`/`exited`. On the remote side going away
  (process exit), the edges simply close; the DAP client sees a plain socket
  close. If the remote adapter sent its own `terminated`/`exited` before exiting,
  those flow through as ordinary payloads — but nothing is invented on its behalf.
- **Reconnect into a reaped session — tombstone; never re-materialize on
  resume.** Materialization is **exclusive to a fresh open**; a *resume* must
  never create a session. The reconnecting edge declares which it is doing:
  - *Resume* (a reconnect — the edge believes its session is still live): if the
    relay has reaped it, return a clear **tombstone** error so the edge learns its
    session is gone, rather than silently landing in a fresh empty session that
    would wait forever for a peer that is never coming back.
  - *Open* (a fresh attach): lazily materialize under first-touch if absent. A
    brand-new debug job uses a fresh UUID and an open, so it never collides with a
    reaped key.

  This makes the open-vs-resume distinction in `Hello` (§7.3) load-bearing, and
  is enforced structurally: only the open path can create a session.
- **Terminate the action on debug-session-end — Decided (MVP: immediate, no
  timer).** When the debug client goes away (the forwarder observes the proxy's
  terminal `Close`), the worker **terminates the action**, immediately and
  unconditionally — no grace period, no configurable timeout. This is both the
  fix for the detach-while-paused worker-slot hazard (a debuggee left suspended at
  a breakpoint cannot pin a slot) and the natural model for a *launched* debuggee:
  like quitting gdb on a program it started, ending the debugger ends the run.
  - The DAP `disconnect`/`terminate` semantics (incl. `terminateDebuggee`) are
    handled **end-to-end** by the real client and adapter, flowing opaquely
    through the relay *before* the socket closes; the worker's terminate is a
    backstop, not the primary mechanism, and needs no DAP awareness.
  - **Known simplification:** this overrides a clean
    `disconnect{terminateDebuggee: false}` "detach and let it finish unattended"
    intent — which we can't see without parsing DAP anyway. Acceptable for the
    MVP; proper **attach-style** semantics (detach-and-keep-running, re-attach)
    are punted to a post-MVP feature.

**Action execution timeout vs. human-speed debugging — Punted (MVP).** A
debuggable action is still subject to the normal REv2 action timeout, which the
worker enforces around the blocking `Runner.Run` call
(`executionTimeout := action.Timeout.AsDuration()`, applied via a context
timeout on the `runner.Run` call in
`bb-remote-execution/pkg/builder/local_build_executor.go`). Interactive
debugging — sitting on a breakpoint, stepping — easily exceeds a normal action's
wall-clock budget, so the worker will cancel the action mid-session when that
timeout fires. This is **independent of the relay's idle/grace logic** (which
correctly keeps a healthy paused session alive, see the idle-timeout note above):
the kill comes from the worker one layer down. **For the MVP we do not solve this
in the service** — we rely on the developer setting a sufficiently long action
timeout on the Bazel invocation that starts the debug build. Automatically
exempting debuggable actions from (or extending) the execution timeout is a
backward-compatible refinement left for later.

**Orphan-timeout default vs. attach-early — Punted (MVP), but the default is
load-bearing.** The store-and-forward model lets the developer's proxy attach
*before* the action exists; the proxy is then the first-touch that materializes
the session and waits for the forwarder. The **orphan timeout** (reap a session
whose second side never connects) does not distinguish "no side present" from
"one healthy side waiting," so if it fires before the action is scheduled +
inputs fetched + the DAP adapter is up on stdio (minutes under queue backlog), it
reaps a perfectly good waiting session. **For the MVP we simply require the
orphan-timeout default to comfortably exceed worst-case farm scheduling latency.**
A cleaner later refinement is to only accrue "orphan" time while *no* side is
attached (a connected-but-waiting proxy should not age toward an orphan reap);
backward-compatible, changes a policy not the protocol.

### 5.5 Triggering debug mode — **Decided**
An action opts into debugging by carrying the `BB_DEBUG_SESSION_ID` environment
variable (§5.1); the worker only spins up a forwarder when that variable is
present.

**Routing debuggable actions to specific workers is out of scope for this
service.** A user who wants only some workers to accept debug forwarding can
already express that with the **existing instance-name / platform-property
mechanism** (e.g. a constant `debug=true` platform property routing to a
dedicated pool, or a dedicated instance name). That is a legitimate, normal use
of the platform key (§5.1) and entirely the deployer's concern — we deliberately
do **not** build any routing or pool-selection logic into the relay, forwarder,
or proxy.

### 5.6 Security / authorization — **Deferred (out of scope for MVP)**
Attaching a debugger to a remote process is privileged. Who is allowed to attach
to which session, and how is that enforced at the relay and the frontend?

**MVP decision: ignore auth.** The MVP relies on the session key being an
unguessable UUID (knowledge of the key is the only gate) and on the relay riding
the frontend's existing transport security. Proper authorization — binding a
session to the identity that submitted the build, and enforcing it at the relay
— is explicitly deferred to a later iteration. This is a security gap we are
accepting only for the MVP.

**The real threat is key *leakage*, not key *guessing*, and the consequence is
severe.** The UUID is not reliably secret: it travels as a `--action_env`
variable, which routinely lands in build logs, the Build Event Protocol stream,
`--execution_log` output, and CI logs. And because the client→server direction
feeds the remote **DAP server** — which can typically evaluate expressions and
control execution — anyone who learns the key can take over the session (a new
`{session_key, side}` stream evicts the incumbent, §6.1) and effectively run code
on the worker. So "unguessable UUID" understates the exposure: the practical risk
is a leaked key, and a leaked key is code-execution-grade. We still accept this
for the MVP, but real authorization (binding the session to the build's submitter
and enforcing it at the relay) should be treated as the first post-MVP security
item, not an optional hardening.

One concrete consequence to record: because a new stream for a
`{session_key, side}` **takes over** from any existing one (the same mechanism
that makes reconnect work, see `relay-protocol.md` §6.1), anyone who knows the
UUID can evict the active proxy/forwarder. This is the same UUID-only-gate gap as
above, not separate new work; closing it falls out of real authorization when we
add it.

## 6. Explicit non-goals (first iteration)

- Supporting DAP adapters that **cannot** speak stdio and only listen on a TCP
  port (bridgeable out-of-band by a user-supplied stdio↔socket wrapper).
- Having the relay understand, validate, or transform DAP messages.
- Multiplexing multiple simultaneous debug sessions per action (revisit later).
- Debugging actions that have already completed.
- Attach-style semantics (detach-and-keep-running, re-attach mid-action): the MVP
  terminates the action when the debugger leaves (§5.4).

## 7. Related documents

This document fixes the high-level shape; the follow-up specs below turn each
decision into a concrete design. They cross-reference each other by the informal
section numbers in parentheses (e.g. "§7.3" = the relay protocol).

- **§7.3 — relay protocol** ([`relay-protocol.md`](relay-protocol.md)). The
  gRPC service, proto messages, sequencing/acks, resume-on-reconnect, and
  termination between an edge and `bb_dap_relay`.
- **relay server** ([`relay-server.md`](relay-server.md)). The `bb_dap_relay`
  server side: per-session state, materialization, buffering/GC, resume,
  termination, and reaping.
- **shared edge-client** ([`edge-client.md`](edge-client.md)). The
  protocol-facing half both edges share: handshake, seq/ack, retain/replay,
  dedupe, reconnect, termination, framing.
- **§7.4 — worker-side forwarder** ([`forwarder.md`](forwarder.md)).
  `side = FORWARDER`: reads `BB_DEBUG_SESSION_ID` from the `Command`, has the
  runner wire the action's stdio to a build-directory socket (teeing stdout to the
  log), bridges it to the relay, terminates the action on debug-session-end.
- **§7.5 — local proxy** ([`proxy.md`](proxy.md)). `side = PROXY`: a stdio DAP
  adapter the DAP client launches; CLI (`--session-id`, `--frontend`), bridges its
  own stdio to the relay via the frontend, terminate-on-detach, attach-timeout UX.
