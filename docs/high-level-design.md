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

- We only support **DAP servers that listen on a TCP port** inside the action
  environment. DAP servers that speak over stdin/stdout are explicitly out of
  scope for the first iteration.
- The new service we are designing is intentionally **dumb**: it is a
  rendezvous buffer that holds DAP messages flowing in each direction between
  the developer and the remote debug-adapter server. It does not parse or
  interpret DAP.

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
   once the action process has exited. See
   `bb-remote-execution/pkg/builder/local_build_executor.go:280`. There is no
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

2. The **remote debug-adapter server** runs as (or alongside) the action inside
   `bb_runner`, listening on a TCP port in the action environment.

3. The **`bb_worker`/`bb_runner` side forwards** that port: it dials the action's
   DAP port and bridges its bytes to/from the new relay service.

4. The **new relay service is a simple bidirectional buffer**. It holds two
   message streams per debug session — developer→server and server→developer —
   and lets each side read what the other has written. It is transport, not
   logic.

## 4. Proposed integration

We introduce one new component plus two thin adapters at the edges.

```
 Developer's machine                     Build farm
 ┌──────────────────────┐
 │  DAP client (VS Code) │
 │          │ TCP        │
 │          ▼            │
 │  Local DAP proxy      │           ┌──────────────────────┐
 │  server (NEW)         │ ───────▶  │   DAP Relay service   │
 │                       │ ◀───────  │   (NEW, "the buffer") │
 └──────────────────────┘  session   │  per-session, two     │
                            keyed     │  directional queues   │
                            stream    └──────────┬───────────┘
                                                 ▲
                                                 │ session-keyed stream
                                                 │
                                      ┌──────────┴───────────┐
                                      │  bb_worker / bb_runner│
                                      │  port-forwarder (NEW) │
                                      │          │ TCP        │
                                      │          ▼            │
                                      │  Remote DAP server    │
                                      │  (the action), on a   │
                                      │  port in the action   │
                                      │  environment          │
                                      └──────────────────────┘
```

### 4.1 New / changed components

| Component | Where it runs | Responsibility |
|---|---|---|
| **DAP Relay service** (new) | On the farm, reachable from both workers and developer machines (like `bb_storage`/`bb_scheduler`) | Per-session bidirectional message buffer. Accepts writes from each side, serves them to the other. No DAP awareness. |
| **Local DAP proxy** (new) | Developer's machine | Listens on a local TCP port for the DAP client. Bridges that connection to the relay service for a given session key. |
| **Worker/runner port-forwarder** (new) | Inside `bb_worker`/`bb_runner`, concurrent with `Runner.Run` | Dials the action's DAP TCP port and bridges it to the relay service for the matching session key. |

### 4.2 Data flow for one debug session

0. The developer generates a session key (UUID) and starts the build with it set
   as an action environment variable (`BB_DEBUG_SESSION_ID`, via `--action_env`),
   passing the same key to the local proxy (see §5.1).
1. The action starts on the runner and brings up a DAP server on a known port.
2. The worker reads `BB_DEBUG_SESSION_ID` from the action's `Command`, and the
   forwarder dials that port and registers with the relay under that session key.
3. The developer points their DAP client at the local proxy; the proxy connects
   to the relay under the same session key.
4. DAP messages flow developer→proxy→relay→forwarder→DAP server and back, with
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
  to an empty queue and never schedules (`in_memory_build_queue.go:512,528`).
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

### 5.2 How the developer's traffic reaches the relay — **Leaning: frontend-hosted gRPC service**

Preferred direction: the relay is a **new gRPC service hosted on the existing
`bb_storage` frontend**, so the developer's proxy connects only to the frontend
it already uses, and gRPC routes to the new service by method name. This mirrors
how `bb_storage` already registers many services (CAS, ByteStream, ActionCache,
Execution, Capabilities) on a single server via one `ServiceRegistrar`
(`bb-storage/cmd/bb_storage/main.go`); a `DebugAdapterRelay` service registers
the same way and reuses the frontend's endpoint, TLS, and auth. The worker-side
forwarder is then just another gRPC *client* of that service, exactly as workers
are already clients of storage.

**This requires an upstream code change — by design.** "Registers the same way"
describes a reusable *pattern*, not a drop-in plugin. The frontend's service list
is a **compile-time closure** passed to `bb_grpc.NewServersFromConfigurationAndServe`
(`bb-storage/cmd/bb_storage/main.go` — each service is an explicit
`Register…Server(s, impl)` call inside the `func(s grpc.ServiceRegistrar)`
callback). There is no config- or plugin-driven service registration: nothing in
`GrpcServers` config can add a service, so a `DebugAdapterRelay` can only join the
server by a `RegisterDebugAdapterRelayServer(s, impl)` call compiled into whatever
binary runs the frontend. Concretely that means either (a) modifying/forking
`bb_storage`'s `main.go` to add the line, or (b) building a custom frontend binary
that imports bb-storage as a library and supplies its own registration closure
covering both the storage services and the relay (the idiomatic Buildbarn
composition pattern). We expect and accept this upstream change; the benefit is
that once registered, the relay transparently inherits the frontend's listen
address, TLS, and auth interceptors — no new endpoint, port, or transport stack.

Still open: whether the relay's per-session buffer lives **in** the frontend
process or in a **separate backend** that the frontend fronts (analogous to how
the frontend fronts the scheduler for Execution), and how that interacts with a
horizontally-scaled, multi-replica frontend (session affinity / shared state).

### 5.3 Concurrency with the blocking `Run` call — **Partially narrowed**
The forwarder must run while `Runner.Run` is in flight.

Network reachability is **not** a blocker: actions are spawned as ordinary child
processes of `bb_runner` (optionally `chroot`'d into the input root,
`cmd/bb_runner/main.go`), with **no network-namespace isolation by default**.
The action's DAP server on `127.0.0.1:<port>` is therefore reachable over
loopback from both `bb_worker` and `bb_runner` on the same host. (Deployments
that add their own network sandboxing — gVisor, bubblewrap with a net ns — would
break this assumption and need revisiting.)

**Port discovery — Decided: deterministic per-thread port from worker config.**
`bb_worker` spawns exactly `RunnerConfiguration.Concurrency` execution threads,
each with a stable `threadID ∈ [0, Concurrency)` (`cmd/bb_worker/main.go:362`,
already surfaced as `workerID["thread"]`), and the entire per-thread executor
stack is constructed inside that loop. We add a `debug_port_range_start` to the
runner configuration and assign each thread the port `start + threadID`:

- The range is naturally per-runner (`[start, start + concurrency)`), since
  `Concurrency` is per-runner.
- The forwarder for a thread learns its port directly from config + its own
  `threadID`. The same per-thread executor injects `BB_DEBUG_PORT = start +
  threadID` into the action's environment (only when the action is debuggable,
  i.e. `BB_DEBUG_SESSION_ID` is present), so the action's DAP server binds the
  port the forwarder will dial. Both derive the number from one source.
- This is a *deterministic* allocation: collision-free across a worker's slots
  with **no free-port probing** (no TOCTOU race), and it avoids the virtual-FS
  visibility concerns of a port-file approach entirely.

Considerations / operator notes:
- **Range coordination.** If a host runs multiple runners or multiple
  `bb_worker` processes, their `[start, start+concurrency)` ranges must not
  overlap; the worker can validate this at startup.
- **Avoid the OS ephemeral range** (Linux ~`32768–60999`) so debug ports don't
  clash with the worker's own outbound sockets.
- **Port reuse across sequential actions** on a thread may hit `TIME_WAIT` on
  rebind; mitigate with `SO_REUSEADDR` on the DAP server (a launch-wrapper
  concern) or tolerate a brief delay.
- **Readiness still needs retry-dial:** the forwarder retries dialing
  `127.0.0.1:<port>` until the server is listening, and gives up when the action
  exits.
- The one real code change is the per-thread `BB_DEBUG_PORT` env injection in the
  executor (which already builds the action's env map and is constructed
  per-thread); it passes through `chroot` fine.

**Forwarder placement — Decided: `bb_worker`.** The forwarder lives in
`bb_worker`, and `bb_runner` is left untouched. The worker is the only component
that knows the per-thread port (concurrency and `threadID` are worker concepts),
it already holds outbound gRPC clients to the farm (so reaching the relay is just
another client), and it owns the goroutine around the blocking `runner.Run` call
(so it can run the forwarder concurrently and tear it down on completion). Doing
this in `bb_runner` would give the intentionally-dumb, less-privileged runner new
network egress and duplicate knowledge it doesn't have. The runner's only
advantages — being the action's direct parent and the one that could enter a
per-action network namespace — are moot today because there is **no netns
isolation by default** (both dial the same loopback). *Caveat that would reopen
this:* a deployment adding per-action network sandboxing would force the dialing
to move into the runner or an in-namespace helper.

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
1. **The edges keep their local TCP sockets open across a relay reconnect.** The
   forwarder's TCP connection to the DAP server's port and the proxy's TCP
   connection to the DAP client stay up while the gRPC stream to the relay drops
   and re-establishes. Neither DAP endpoint observes a disconnect — only a pause.
2. **Sequence numbers + replay** ensure the reconnecting side resumes exactly
   where it left off, with no lost or duplicated messages.

**Message framing.** Messages are framed at the **edges, not in the relay**. DAP
uses LSP-style framing — a `Content-Length: <n>\r\n\r\n` header followed by `n`
bytes of JSON — which is trivial to parse (read the header, read `n` bytes; no
JSON parsing needed). The proxy and forwarder each read a *complete* DAP message
off their TCP socket and hand it to the relay as one discrete payload, and in
the reverse direction re-frame a discrete payload back onto TCP. The relay
therefore deals only in **discrete opaque payloads** — message-oriented (so a
reconnect never resumes mid-message) while remaining fully DAP-agnostic.

**Handshake / who waits.** DAP's handshake is client-driven: the client sends
`initialize`, then `launch`/`attach`, and the remote side (the DAP *server*)
only responds. Because the buffer holds the client's messages until the
executor-side forwarder drains them, the normal DAP path "just works" with no
custom sequencing. *Caveat:* DAP clients impose a timeout on the
`initialize`/`attach` request; if the executor side is slow to come up
(scheduling queue + input fetch before the DAP server is listening), the client
may time out. This is a client-config / proxy-UX concern (attach timeout,
"connecting…" affordance), not a relay design issue.

Implied protocol shape: **one bidirectional streaming RPC** that both edges
call; the first message declares `{session_key, side}`; subsequent messages are
`{seq, payload}` (one whole DAP message) plus acknowledgements; the relay keeps a
per-direction sequence-numbered log, forwards to the connected peer, and replays
unacked messages on reconnect.

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
  **local TCP socket** (the DAP client or DAP server — "the right place").
  Cumulative: "I have through N." The relay then drops everything `≤ N` in that
  direction. Under healthy flow the retained window is a few messages; it grows
  only when a peer is slow or absent.
- **Resume point comes from the receiver, not the relay's stored ack.** On
  (re)connect the receiving side announces its **high-water mark** (highest
  contiguous `seq` it has durably written to TCP) as the first message, and the
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
  HTTP/2 flow control *bounds* the unacked window and propagates backpressure to
  the source TCP (the live DAP endpoint pauses rather than the relay growing
  unbounded); stream EOF/errors and keepalive PINGs let the relay detect a
  disconnect and switch from GC to *retain-for-replay*.
- **Ack boundary rationale:** "written to local TCP" is the strictest signal
  available — DAP has no transport-level ack, and if an edge dies after writing,
  its TCP connection dies too, so the peer DAP endpoint sees a disconnect and the
  session is finished anyway.
- **Other GC levers (kept):** *piggybacked acks* (carry the ack `seq` on data
  flowing the other way — an optimization); *whole-session teardown GC* (drop the
  entire buffer on session end); and a **hard-cap safety valve** — if the unacked
  window exceeds a configured size/age, the session is **aborted** (evicting an
  unacked message would corrupt the DAP session, so the cap must abort, not
  silently drop).

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

Still open:
- **Backpressure-vs-abort threshold.** The concrete cap (bytes/messages/age) at
  which the safety valve fires, and whether the relay blocks (pure backpressure)
  right up to that cap.
- **Session teardown / GC.** Materialization is settled (above); teardown is not.
  When is a session garbage-collected — on action exit, on explicit DAP
  `disconnect`, on an idle timeout after both sides leave?
- **What happens to an in-flight session when the action exits or times out**
  (the DAP server goes away) — surface a clean DAP `terminated`/`exited` to the
  client, or just close?

### 5.5 Triggering debug mode — **Partially decided**
An action opts into debugging by carrying the `BB_DEBUG_SESSION_ID` environment
variable (§5.1); the worker only spins up a forwarder when that variable is
present. Still open: whether a **constant** platform property (e.g.
`debug=true`) should additionally be used to route debuggable actions to a
dedicated, debug-enabled worker pool. Unlike a per-session value, a constant
property is a legitimate use of the platform key (§5.1) and is the right tool
*if* only some workers should accept inbound debug forwarding. This is a
deployment choice we can defer.

### 5.6 Security / authorization — **Deferred (out of scope for MVP)**
Attaching a debugger to a remote process is privileged. Who is allowed to attach
to which session, and how is that enforced at the relay and the frontend?

**MVP decision: ignore auth.** The MVP relies on the session key being an
unguessable UUID (knowledge of the key is the only gate) and on the relay riding
the frontend's existing transport security. Proper authorization — binding a
session to the identity that submitted the build, and enforcing it at the relay
— is explicitly deferred to a later iteration. This is a security gap we are
accepting only for the MVP.

## 6. Explicit non-goals (first iteration)

- Supporting DAP servers that communicate over stdin/stdout.
- Having the relay understand, validate, or transform DAP messages.
- Multiplexing multiple simultaneous debug sessions per action (revisit later
- Debugging actions that have already completed.

## 7. Next steps

1. Agree on the high-level shape in this document.
2. ~~Pick a session-identity scheme~~ — decided: env-var session key (§5.1).
   Confirm the relay reachability model (§5.2) — frontend-hosted service, with
   the in-process-vs-backend question still open.
3. Define the relay service's gRPC/streaming protocol and proto messages.
4. Specify the worker/runner forwarder and how it discovers the action's port
   (§5.3) — including reading `BB_DEBUG_SESSION_ID` from the `Command`.
5. Specify the local proxy and how a developer configures/launches it (the CLI
   takes the session key and the frontend endpoint).
