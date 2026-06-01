# DAP Relay — Local DAP Proxy

> Status: **Draft for refinement.** This is the §7.5 follow-up. It specifies the
> developer-side proxy, which presents a remote debug session to a local DAP
> client (VS Code, etc.) as if it were an ordinary local debug adapter. It builds
> on the **edge-client** (`edge-client.md`) as `side = PROXY`. Section references
> like "§5.2" point at `high-level-design.md`; "edge-client §X" points at
> `edge-client.md`; "protocol §X" at `relay-protocol.md`.

## 1. Role and placement

The proxy runs on the **developer's machine** as a **stdio DAP adapter**: the DAP
client (VS Code, etc.) launches it as a child process and speaks DAP over its
**stdin/stdout**, exactly as it would any normal, locally-installed debug adapter.
The proxy bridges that stdio to the relay, hiding all of the remoting (§3 goals).
The proxy embeds an edge-client (`side = PROXY`); everything protocol-facing is
the edge-client's, so the proxy itself is just the CLI, the stdio bridge, and the
developer-facing UX.

It is the mirror of the forwarder: same edge-client, opposite `side`, and both
bridge a **stdio** connection — the proxy its own stdin/stdout (to the DAP client),
the forwarder the action's stdin/stdout (to the DAP adapter).

## 2. CLI and configuration

The DAP client launches the proxy as its adapter (names TBD; `bb_dap_proxy` as a
placeholder), passing via the launch config:

- **`--session-id=<uuid>`** — the same `BB_DEBUG_SESSION_ID` UUID the developer
  generated and passed to Bazel via `--action_env` (§5.1). This is the relay
  `session_key`; the developer is responsible for using the same value on both
  sides.
- **`--frontend=<endpoint>`** (+ TLS/credentials) — the `bb_storage` frontend gRPC
  endpoint to reach the relay through (§5.2).

The proxy speaks DAP over the stdin/stdout the DAP client gives it when it spawns
the adapter. In a VS Code launch config this is the adapter's command/args (e.g. a
`debugAdapterExecutable` / `"type"`-registered adapter).

## 3. Reaching the relay

The proxy connects to the relay **through the `bb_storage` frontend** (§5.2) — the
single endpoint it already trusts — which routes the `DebugAdapterRelay` service
and forwards its stream to `bb_dap_relay`. The proxy is just a gRPC client of that
service and inherits the frontend's transport security (and, eventually, auth;
none in the MVP, §5.6). Unlike the forwarder (which dials `bb_dap_relay`
directly), the proxy **always** goes via the frontend.

## 4. Lifecycle

1. **Start**: the DAP client spawns the proxy. Parse the CLI and prepare the relay
   gRPC client to the frontend.
2. **Bind stdio**: hand the proxy's own stdin/stdout to the edge-client
   (edge-client §11) with `side = PROXY`, `session_key` from `--session-id`, the
   frontend relay access, and the local-close reason `LOCAL_PEER_DISCONNECTED`.
   (There is no accept step — the DAP client is already connected via the stdio it
   launched the proxy with.)
3. **Bridge**: the edge-client connects to the relay (`Open`, materializing the
   session via lazy first-touch if the forwarder has not yet attached, §5.4) and
   runs the protocol until a terminal outcome (§5). The client's `initialize` /
   `attach` and subsequent messages flow through; if the executor side is not up
   yet, they buffer at the relay and the normal DAP path "just works" once the
   forwarder attaches (§5.4) — modulo the attach-timeout caveat (§6).

**One session per invocation (MVP).** Each launch of the proxy serves one DAP
client (the one that spawned it) for its `session_key`, then exits when that
session ends. This is a natural consequence of terminate-on-detach (§5.4
"Terminate the action on debug-session-end"): once the DAP client disconnects, the
action is terminated, so there is nothing to re-attach to. Debugging again means a
fresh build with a fresh UUID — and, since the client launches the proxy, a fresh
proxy process.

## 5. Termination

Driven by the edge-client's terminal outcome (edge-client §9.4):

- **`PeerClosed`** (the action exited / the forwarder sent `Close`): the
  edge-client has already written all preceding payloads and then closes its stdio
  to the DAP client (the proxy exits) — the DAP client sees its session end. Nothing
  is synthesized (§5.4); any `terminated`/`exited` the remote adapter emitted
  already arrived as ordinary payloads. The proxy reports the end to the developer
  (e.g. "session ended — process exited, code N", from the `Close` detail) and
  exits.
- **`LocalClosed`** (the developer stopped debugging — the DAP client disconnected
  first): the edge-client emits the terminal `Close{LOCAL_PEER_DISCONNECTED}`,
  which reaches the forwarder and causes the worker to terminate the action
  (§5.4 "Terminate the action on debug-session-end"). The proxy reports and exits.
- **`Tombstone`** (`NOT_FOUND`): a reconnect found the session reaped (e.g. the
  action exited and was reaped during a relay-stream gap). The edge-client closes
  its stdio to the DAP client; the proxy reports "session is gone" and exits. (The first
  connect is always `Open`, so this only arises on a post-drop `Resume`,
  protocol §7.3.)
- **`Aborted` / `CapExceeded`**: the relay tore the session down (reap, supersede,
  or byte cap). The proxy reports the cause and exits.

## 6. Attach-timeout UX

DAP clients impose a timeout on the `initialize`/`attach` request (§5.4). Because
the remote side may be slow to materialize — scheduling queue + input fetch before
the action's DAP adapter is even running on stdio — the client's `attach` can time out
while its request sits buffered at the relay. This is a **client-config / UX**
concern, not a relay or proxy protocol issue (the proxy never fabricates a
response, consistent with the no-synthesize decision, §5.4):

- Guidance: developers should configure a **generous attach timeout** in their
  launch config for remote debug sessions.
- The proxy should surface a clear **"connecting / waiting for the remote session"**
  affordance (log/status) so a long wait is legible rather than looking hung.

## 7. Relay/frontend failures

Symmetric to the forwarder's open coupling question (forwarder §8), but
developer-facing. If
the frontend/relay is unreachable, the edge-client keeps retrying (reconnect
backoff, edge-client §8) while the DAP client's stdio is open; the DAP client sees
only a pause. Because failures here are seen by a human, the proxy should emit
**clear diagnostics** — cannot reach frontend, TLS/cert problems, session gone —
rather than silently retrying forever with no feedback.

## 8. End-to-end developer workflow

Tying together §4.2 of the high-level design:

1. Generate a UUID: `export BB_DEBUG_SESSION_ID=$(uuidgen)`.
2. Start the build so the target action is debuggable, e.g.
   `bazel build //target --action_env=BB_DEBUG_SESSION_ID` (and whatever makes the
   action run a DAP adapter on stdio, e.g. `lldb-dap`, §forwarder).
3. Configure the DAP client (a VS Code launch config) to use the proxy as its
   adapter command:
   `bb_dap_proxy --session-id=$BB_DEBUG_SESSION_ID --frontend=<endpoint>`.
4. Start debugging; the client launches the proxy and speaks DAP over its stdio.

The developer must ensure exactly **one** debuggable action carries the
`session_key` (scope the `--action_env` to a single target/test); multiplexing is
out of scope (§6 non-goals) and two forwarders sharing one key would just take
over from each other (protocol §6.1).

## 9. Security

- **No local network surface.** As a stdio adapter the proxy opens **no** local
  listening socket; it talks to the DAP client over inherited stdio and only dials
  *outbound* to the frontend.
- Beyond that, the MVP's only access gate is the unguessable `session_key` UUID
  and the frontend's transport security (§5.6); real authorization is deferred.

## 10. Open questions

- **Proxy name / packaging** — final binary name, and whether it ships standalone
  or as a subcommand of an existing Buildbarn client tool.
- **Pre-warming the session** — connect to the relay immediately on startup (to
  materialize early), or only once the DAP client's first message arrives (current
  MVP)? No protocol benefit given store-and-forward; purely a UX/latency
  micro-question.
