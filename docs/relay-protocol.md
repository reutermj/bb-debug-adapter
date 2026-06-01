# DAP Relay — gRPC/Streaming Protocol

> Status: **Draft for refinement.** This is the §7.3 follow-up to
> `docs/high-level-design.md`. It turns the decisions recorded in that document
> (notably §5.4) into a concrete gRPC service and message grammar. Section
> references like "§5.4" point at the high-level design doc unless stated
> otherwise.

## 1. Scope

This document specifies the wire protocol between a **relay edge** (either the
developer-side **proxy** or the worker-side **forwarder**) and the **relay**
(`bb_dap_relay`). It defines the gRPC service, the stream message grammar,
sequencing/acknowledgement, resume-on-reconnect, and termination.

It does **not** specify the proxy or forwarder internals (those are §7.4/§7.5),
nor anything inside the DAP payloads — the relay is DAP-agnostic (§5.4) and
treats every payload as opaque bytes.

## 2. Design inputs (what this protocol must satisfy)

Recap of the already-made decisions this grammar is built around:

- **Store-and-forward, message-oriented** (§5.4). Two independent directions per
  session, each an ordered log of discrete whole DAP messages with a
  per-direction sequence number.
- **Framing at the edges, not the relay** (§5.4). An edge reads the LSP
  `Content-Length` header only far enough to delimit one complete message, then
  hands the relay that **whole message verbatim — header bytes included** — as a
  single opaque payload. The consuming edge writes those bytes straight back to
  TCP with no re-framing. The relay never parses DAP, and the edges never
  reconstruct framing (the original bytes are preserved exactly).
- **Disconnect tolerance via sequence numbers + replay** (§5.4). A transient gRPC
  stream drop does not perturb the DAP endpoints; the edge reconnects and
  resumes exactly where it left off, with no loss or duplication.
- **Receiver-owned resume; cumulative acks** (§5.4). The resume point comes from
  the receiver's high-water mark, *not* the relay's stored ack. Receivers dedupe
  defensively.
- **"`Send()` is not delivery" on every hop** (§5.4). Each sender retains its
  sent-but-unconfirmed messages until its downstream confirms custody.
- **Lazy first-touch, symmetric materialization** (§5.4). The first edge to
  connect with a `session_key` materializes the session; no `CreateSession` RPC.
- **Terminal-close marker + drain-then-drop teardown** (§5.4). A terminal close
  (local TCP gone) is distinct from a transient stream drop and is ordered after
  the last message in its direction.
- **One shared RPC + `side` field as the MVP default** (§5.4, relaxed). Not a
  hard requirement; the load-bearing invariant is a single shared message
  grammar so one client library serves both edges.
- **Single `bb_dap_relay` node; frontend demuxes and forwards** (§5.2).
- **Hard-coded byte cap, abort on exceed; no auth** (§5.4, §5.6) for the MVP.

## 3. Topology and transport

```
 proxy  ─┐                                  ┌─ forwarder
         │   Attach (bidi stream)           │   Attach (bidi stream)
         ▼                                  ▼
   ┌───────────────┐   transparent    ┌──────────────┐
   │ bb_storage    │   gRPC forward    │ bb_dap_relay │
   │ frontend      │ ───────────────▶ │ (single node)│
   │ (demux only)  │ ◀─────────────── │              │
   └───────────────┘                   └──────────────┘
```

- Both edges call the **same `Attach` RPC**; only the endpoint they dial
  differs. The **proxy** dials the `bb_storage` frontend, which demuxes
  `DebugAdapterRelay` by method name (§5.2) and **transparently forwards** the
  bidirectional stream to the single `bb_dap_relay` node — it does **not** parse
  stream messages. The protocol is therefore defined **end-to-end between an edge
  and the relay**; the frontend is a passthrough. This is why §5.4 reasons about
  "two hops around the relay" (`forwarder → relay`, `relay → proxy`) and treats
  the frontend as transparent.
- The worker forwarder dials `bb_dap_relay` **directly** — it is an internal
  farm component (§5.2, forwarder §6), not via the frontend; the proto is
  identical either way. The developer proxy always goes via the frontend (the
  single endpoint it already trusts).
- Transport security and (eventually) auth are inherited from the frontend
  (§5.2); auth is out of scope for the MVP (§5.6).

## 4. Core concepts

### 4.1 Session, directions, sequence numbers

A **session** is identified by `session_key` (a developer-generated UUID, §5.1)
and holds two independent **directions**:

- **client→server** (`C2S`): DAP client → proxy → relay → forwarder → DAP server.
- **server→client** (`S2C`): DAP server → forwarder → relay → proxy → DAP client.

Each direction is an ordered log of frames carrying a **per-direction sequence
number** (`seq`), assigned by that direction's **producer**, monotonic from 1.
The two directions have independent seq spaces.

### 4.2 Outbound / inbound per edge

Rather than label `C2S`/`S2C` on the wire, the protocol is expressed relative to
the connecting edge. Every edge stream has exactly:

- an **outbound** direction (the one this edge *produces*), and
- an **inbound** direction (the one this edge *consumes*).

The `side` in `Hello` fixes the mapping:

| `side`      | edge       | outbound | inbound |
|-------------|------------|----------|---------|
| `PROXY`     | proxy      | `C2S`    | `S2C`   |
| `FORWARDER` | forwarder  | `S2C`    | `C2S`   |

This is what makes the grammar symmetric — both edges run identical logic; only
the direction mapping differs.

### 4.3 Custody and the two ack boundaries

There are two reliable hops around the relay, and the *meaning* of "I have
through N" differs by node (§5.4):

- **Delivery ack** — an **edge** acks its **inbound** direction once it has
  written that message to its **local TCP** (the DAP client or DAP server, "the
  right place"). This is what lets the **relay GC** its buffer for that
  direction.
- **Receipt ack** — the **relay** acks an edge's **outbound** direction once it
  has taken the frame into its own buffer. This is what lets the **edge drop**
  its retransmit copy.

Both are carried by the same `Ack` wire message; only the custody semantics
differ. Every sender retains sent-but-unacked frames; every receiver sends
cumulative acks.

> Note on durability: with a single, non-replicated relay node (§5.2), a relay
> **process crash** ends active sessions — disconnect tolerance covers stream
> drops (network blips, frontend restarts), not relay-process loss. That is an
> accepted MVP limitation.

## 5. The proto

```proto
syntax = "proto3";

package buildbarn.daprelay.v1;

// option go_package = TBD (module path not yet fixed).

// DebugAdapterRelay is the single relay service. One Attach stream carries one
// edge's participation in one debug session. Both the developer-side proxy and
// the worker-side forwarder call Attach; Hello.side distinguishes their role.
//
// Whether to keep one Attach RPC (current default) or split into per-role RPCs
// later is an open, reversible API-surface choice (§5.4); the message grammar
// below is what is load-bearing.
service DebugAdapterRelay {
  rpc Attach(stream EdgeToRelay) returns (stream RelayToEdge);
}

// Messages sent from an edge to the relay.
message EdgeToRelay {
  oneof msg {
    Hello hello = 1;  // exactly once, as the first message on the stream
    Frame frame = 2;  // an outbound frame (this edge -> peer)
    Ack   ack   = 3;  // delivery-ack of inbound frames (this edge consumed them)
  }
}

// Messages sent from the relay to an edge.
message RelayToEdge {
  oneof msg {
    Frame frame = 1;  // an inbound frame (peer -> this edge)
    Ack   ack   = 2;  // receipt-ack of this edge's outbound frames
  }
}

// First message on every Attach stream. Declares the session and role, and
// whether this is a fresh attach or a reconnect. Materialization (lazy
// first-touch) happens ONLY on the Open path — a Resume never creates a session.
message Hello {
  string session_key = 1;
  Side   side        = 2;

  oneof attach {
    Open   open   = 3;  // fresh attach; materialize (first-touch) if absent
    Resume resume = 4;  // reconnect to an EXISTING session; never creates
  }
}

// Which edge this stream is. Named for the role on the build, not the DAP role,
// to avoid colliding with gRPC "client/server" (both edges are gRPC clients of
// the relay).
enum Side {
  SIDE_UNSPECIFIED = 0;
  PROXY     = 1;  // developer-side proxy; produces client->server
  FORWARDER = 2;  // worker-side forwarder; produces server->client
}

// Fresh attach. The edge does not assume a pre-existing session: if absent, the
// relay materializes it (lazy first-touch, §5.4); if present, the edge attaches.
// Carries no high-water — a fresh attach has consumed nothing.
message Open {}

// Reconnect to a session the edge believes is still live. The relay attaches to
// the EXISTING session and resumes inbound delivery from
// inbound_delivered_through + 1. It NEVER materializes: if the relay has no such
// session (reaped, or never existed), it fails the stream with NOT_FOUND (the
// "tombstone"), so the edge learns its session is gone instead of silently
// landing in a fresh empty one (§5.4). Re-materialization is reserved for Open.
message Resume {
  // Highest INBOUND seq this edge has durably handled (written to local TCP).
  // The relay resumes inbound delivery from here + 1.
  uint64 inbound_delivered_through = 1;
}

// One entry in a direction's log. Frames flow up (outbound) on EdgeToRelay and
// down (inbound) on RelayToEdge; the relay forwards them preserving seq.
message Frame {
  uint64 seq = 1;  // per-direction, monotonic from 1, assigned by the producer

  oneof kind {
    bytes payload = 2;  // one whole DAP message, carried verbatim INCLUDING its
                        // LSP Content-Length frame; written to TCP unchanged
    Close close   = 3;  // terminal end of this direction (ordered after last payload)
  }

  // Reserved for a future optimization (§5.4 "piggybacked acks"): cumulative ack
  // for the direction flowing the other way on this stream. MVP senders set this
  // to 0 and send standalone Ack messages only; MVP receivers ignore it. Emitting
  // it later must stay purely additive (never a substitute for standalone Acks
  // unless the receiver is known to consume it), so it cannot silently stall GC.
  uint64 piggyback_ack_through = 4;
}

// Graceful terminal close of a direction, produced by that direction's producer
// when its local TCP closes. ALL fields are informational only (for logs and
// telemetry) — neither the relay nor the edges turn any of them into a DAP
// terminated/exited message (§5.4).
message Close {
  CloseReason reason = 1;

  // Action exit code, set by the forwarder on PROCESS_EXITED when known
  // (Runner.Run surfaces it). Unset/absent otherwise (e.g. proxy-side close).
  optional int32 exit_code = 2;

  // Free-form human-readable detail for logs (e.g. "killed by signal 9",
  // "client closed socket"). Never parsed; safe to leave empty.
  string detail = 3;
}

enum CloseReason {
  CLOSE_REASON_UNSPECIFIED = 0;
  LOCAL_PEER_DISCONNECTED  = 1;  // proxy side: the DAP client closed its socket
  PROCESS_EXITED           = 2;  // forwarder side: the action / DAP server exited
}

// Cumulative acknowledgement. "I have taken everything through `through` into
// custody." Sent by a receiver; lets the corresponding sender drop frames <=
// `through`. Custody = "written to local TCP" for an edge (delivery ack),
// "buffered" for the relay (receipt ack).
message Ack {
  uint64 through = 1;
}
```

## 6. Connection lifecycle

### 6.1 Handshake

1. Edge opens `Attach` and sends `Hello{session_key, side, open|resume}`.
2. Relay resolves the session per the `attach` variant:
   - `Open`, session absent → **materialize** it (first-touch). This is the
     **only** path that creates a session.
   - `Open` or `Resume`, session present → attach. If another stream is already
     attached for the same `{session_key, side}`, the **new stream takes over**
     and the old one is closed with `ABORTED` ("superseded").
   - `Resume`, session absent → fail the stream with **`NOT_FOUND`** (tombstone).
     A `Resume` **never** materializes.
3. Relay immediately sends `Ack{through = outbound high-water}` — i.e. the
   highest seq it has already buffered for this edge's outbound direction. This
   tells the producer its **resume point**: drop retained outbound ≤ `through`,
   then (re)send from `through + 1`.
4. Relay (re)starts delivering inbound frames from
   `inbound_delivered_through + 1` (which is `0 + 1 = 1` for an `Open`).

Both resume points are thus exchanged at the handshake: the edge tells the relay
where to resume *inbound delivery* (via `Resume.inbound_delivered_through`; an
`Open` implies 0), and the relay tells the edge where to resume *outbound
production* (via the first `Ack`).

### 6.2 Steady state

- **Producing:** edge sends `Frame{seq, payload}` up for each whole DAP message
  read off its local TCP, assigning the next outbound seq. It retains each frame
  until a receipt-`Ack` covers it.
- **Consuming:** edge receives inbound `Frame`s, **dedupes** any
  `seq ≤ inbound_delivered_through` (defensive backstop, §5.4), writes each
  payload (already a fully-framed DAP message) to its local TCP verbatim **in seq
  order**, and sends a delivery-`Ack{through}` as it does.
- The relay forwards frames each way, sends receipt-acks for what it has
  buffered, applies delivery-acks to GC, and bounds memory via HTTP/2 flow
  control (backpressure to the live DAP endpoint, §5.4).
- **MVP: standalone `Ack` messages only.** Debug traffic is low-volume and
  interactive, so the piggyback optimization buys nothing; `piggyback_ack_through`
  stays 0 and is ignored (it is reserved for a later revision).

### 6.3 Reconnect (transient drop)

A stream that ends **without** an in-band `Close` and **without** a terminal
status (§7) is a transient drop. The edge keeps its local TCP open, reconnects,
and sends `Hello{Resume{inbound_delivered_through = <last written>}}` — a
reconnect always uses `Resume`, never `Open`, so a session the relay has already
reaped surfaces as a `NOT_FOUND` tombstone rather than silently re-materializing.
The handshake (6.1) re-establishes both resume points; retained frames are
replayed; dedupe absorbs any overlap. Neither DAP endpoint observes more than a
pause.

> **Invariant (MUST):** a reconnect uses `Resume` with the edge's true inbound
> high-water; `Open` is only for a side's genuine first attach. Misusing `Open`
> to reconnect implies "deliver my inbound from seq 1," but the relay has already
> GC'd frames the peer acked, so it cannot replay them — an unfillable gap. The
> MVP edges (proxy and forwarder are both our code) simply obey this, and the
> relay does **not** defend against a violating `Open`. A defensive relay-side
> check (reject/normalize an `Open` for a side that already has delivery
> progress) is **deferred** (§10) — note that a process *crash* of an edge is not
> a reconnect at all (its local TCP dies, ending the session), so there is no
> legitimate `Open`-after-progress case to handle for the MVP.

## 7. Termination

Two distinct termination paths, distinguished so the consuming edge knows whether
to reconnect:

### 7.1 Graceful close (drain-then-drop)

When an edge's **local TCP closes** (DAP client quit, or action/DAP server
exited), its producer sends a terminal `Frame{seq, close}` as the **final entry**
in its outbound direction, ordered after the last payload. The relay:

1. continues delivering that direction's buffered frames — **including the
   `Close`** — to the consuming edge, and
2. reaps the session only **after** the consumer's delivery-ack covers the
   `Close` seq (drain-then-drop, §5.4), or a timeout fires (§8).

The consuming edge, upon writing all preceding payloads and seeing the inbound
`Close`, closes its local TCP and **does not reconnect**. No DAP `terminated`/
`exited` is synthesized (§5.4) — the DAP endpoint simply sees its socket close,
and any `terminated`/`exited` the remote adapter actually sent rode through as
ordinary payloads beforehand.

Asymmetry on action exit (`PROCESS_EXITED`): new `C2S` frames have nowhere to go
(the DAP server is gone) and are refused/discarded, while buffered `S2C` frames
must still drain to the proxy.

### 7.2 Relay-originated termination (abrupt)

Conditions the relay raises by **failing the stream with a gRPC status** (no
drain). The edge treats these as terminal: close local TCP, do **not** reconnect.

| Condition | Status | Meaning |
|---|---|---|
| `Resume` on absent/reaped session | `NOT_FOUND` | Tombstone — your session is gone (§5.4). `Resume` never creates. |
| Byte cap exceeded | `RESOURCE_EXHAUSTED` | Unacked buffer over the hard cap; session aborted (§5.4). Cannot drop unacked frames without corrupting DAP, so the cap aborts. |
| Idle / orphan reap while connected | `ABORTED` | Reaped by a lifecycle timeout (§8). |
| Superseded by a newer stream | `ABORTED` | Another stream attached for the same `{session_key, side}`. |

**Terminal vs. transient — the edge's rule.** The edge decides whether to
reconnect purely from how the stream ended:

- **Terminal (do not reconnect; close local TCP):** an in-band `Close` for the
  inbound direction (§7.1), or a stream that fails with `NOT_FOUND`,
  `RESOURCE_EXHAUSTED`, or `ABORTED`.
- **Transient (reconnect with `Resume`, §6.3):** any other stream/transport
  failure — `UNAVAILABLE`, `DEADLINE_EXCEEDED`, connection reset, keepalive
  timeout, etc.

`NOT_FOUND` and `RESOURCE_EXHAUSTED` are deliberately *not* in the transient set
even though a naive client might retry them — retrying a tombstoned or
cap-aborted session cannot succeed. `DEADLINE_EXCEEDED` is treated as transient
(transport-level), which is why it is **not** reused for lifecycle reaps; those
use `ABORTED`.

**`ABORTED` and your own reconnect.** During a normal reconnect the relay closes
the *old* stream with `ABORTED` ("superseded") — but that is the edge superseding
*itself*. An edge therefore treats `ABORTED` on a stream it has **already
replaced** as cleanup to ignore, and `ABORTED` on its **current/only** stream as
terminal (it was reaped, or superseded by someone else under the §5.6 no-auth
gap). The distinction is "do I have a newer stream?", not the status code.

### 7.3 Reaped-session reconnect (tombstone, never re-materialize)

This is the §5.4 decision encoded structurally in the `Hello.attach` oneof —
**materialization is exclusive to `Open`; `Resume` can never create**:

- A reconnecting edge always sends `Resume` (it believes its session is live). If
  the relay has reaped it, the edge gets `NOT_FOUND` and learns the session is
  gone, rather than silently landing in a fresh empty session that would wait
  forever for a peer that is never coming back.
- Materialization happens only on a fresh `Open`. A brand-new debug job uses a
  fresh UUID and `Open`, so it materializes cleanly; it does not collide with a
  reaped key. (The relay keeps no tombstone records — "absent" simply means "no
  live session," and only `Open` may create one.)

## 8. Timeouts and resource bounds

- **Reconnect-grace timeout** — how long the relay retains a session after a
  stream drops with neither an in-band `Close` nor a terminal status, awaiting
  reconnect before reaping. (Configurable, §5.4.)
- **Orphan timeout** — materialized, but the second side never connects.
  (Configurable.)
- **Idle timeout** — fires only on a session whose edges are **disconnected** (no
  live stream / no keepalive heartbeat), never on a healthy-but-quiet one. A
  developer paused at a breakpoint with both streams up is fully live and is not
  reaped regardless of how long there is no DAP traffic. (Configurable.)
- **Byte cap** — hard-coded maximum retained (unacked) bytes per session; exceed
  ⇒ abort with `RESOURCE_EXHAUSTED` (§5.4). Not configurable in the MVP.
- **Backpressure** — below the cap, HTTP/2 flow control pauses the source DAP
  endpoint rather than growing the buffer unbounded (§5.4).

**Liveness detection — gRPC keepalive PINGs; no application heartbeat.** A
half-open stream (peer vanished without a clean close) is detected via HTTP/2
keepalive PINGs, configured on both the edge and the relay; the resulting stream
error is what flips the relay from GC to *retain-for-replay* and starts the
reconnect-grace timer, and what tells an edge to reconnect. We add **no**
application-level heartbeat message for the MVP — it would be redundant with
keepalive and add DAP-agnostic chatter. Keepalive timing is configurable; the
keepalive timeout should be comfortably shorter than the reconnect-grace and idle
timeouts so a dead stream is noticed well before a session would be reaped. (Set
the relay's server-side keepalive enforcement policy to permit the edges' client
keepalives.)

When the debug session ends (the forwarder sees the proxy's terminal `Close`),
the *worker* terminates the action immediately (§5.4) — this also covers the
detach-while-paused worker-slot hazard. It is worker behavior, not part of this
protocol.

## 9. Deliberately out of scope (MVP)

- **Auth / authorization** (§5.6) — knowledge of the UUID is the only gate.
- **DAP awareness** — the relay never parses, validates, or transforms payloads.
- **Framing in the relay** — payloads are pre-framed discrete DAP messages.
- **Multiple sessions per action / multiple peers per side** — a second stream
  for a `{session_key, side}` is a takeover, not multiplexing.
- **Surviving a relay-process crash** — single non-replicated node (§5.2).

## 10. Refinement questions

### Resolved
- **Piggybacked acks** — field reserved (`Frame.piggyback_ack_through`) but
  **unused in the MVP**; standalone `Ack` only (§6.2). Emitting it later must stay
  purely additive.
- **`Close` detail fields** — **yes**, informational only: `Close` carries an
  optional `exit_code` and a free-form `detail` string for logs/telemetry,
  never turned into DAP (§5 proto).
- **Status codes** — `NOT_FOUND` (tombstone), `RESOURCE_EXHAUSTED` (byte cap),
  `ABORTED` (reap/supersede). Terminal-vs-transient rule spelled out in §7.2.
  `FAILED_PRECONDITION` rejected for tombstone (retry can't help) and
  `DEADLINE_EXCEEDED`/`CANCELLED` avoided for reaps (they read as transient).
- **Heartbeats** — gRPC keepalive PINGs; **no** application heartbeat (§8).

### Still deferred
- **Frontend↔relay hop** — taken as a transparent gRPC forward of the same proto
  (no internal proto). Still to *validate* against the frontend's existing
  forwarding facilities when we build it.
- **Defensive handling of a misused `Open`** (§6.3) — whether the relay should
  reject/normalize an `Open` for a `{session_key, side}` that already has delivery
  progress, rather than trusting edges to reconnect with `Resume`. Out of scope
  for the MVP (we own both edges); revisit if third-party edges appear.
