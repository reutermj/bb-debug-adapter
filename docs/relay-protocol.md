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

- Both edges call the **same `Attach` RPC** on the `bb_storage` frontend's gRPC
  endpoint. The frontend demuxes `DebugAdapterRelay` by method name (§5.2) and
  **transparently forwards** the bidirectional stream to the single
  `bb_dap_relay` node — it does **not** parse stream messages. The protocol is
  therefore defined **end-to-end between an edge and the relay**; the frontend is
  a passthrough. This is why §5.4 reasons about "two hops around the relay"
  (`forwarder → relay`, `relay → proxy`) and treats the frontend as transparent.
- A deployment **may** let the worker forwarder dial `bb_dap_relay` directly
  (it is an internal farm component) instead of via the frontend; the proto is
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

| `side`   | edge       | outbound | inbound |
|----------|------------|----------|---------|
| `CLIENT` | proxy      | `C2S`    | `S2C`   |
| `SERVER` | forwarder  | `S2C`    | `C2S`   |

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

enum Side {
  SIDE_UNSPECIFIED = 0;
  CLIENT = 1;  // developer-side proxy; produces client->server
  SERVER = 2;  // worker-side forwarder; produces server->client
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

  // Optional optimization (§5.4 "piggybacked acks"): cumulative ack for the
  // direction flowing the other way on this stream. 0 means "no piggybacked
  // ack"; MVP implementations may ignore this and use standalone Ack only.
  uint64 piggyback_ack_through = 4;
}

// Graceful terminal close of a direction, produced by that direction's producer
// when its local TCP closes. Reason is informational only — neither the relay
// nor the edges synthesize any DAP terminated/exited message (§5.4).
message Close {
  CloseReason reason = 1;
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
- `Ack`s may be sent standalone or piggybacked (`piggyback_ack_through`).

### 6.3 Reconnect (transient drop)

A stream that ends **without** an in-band `Close` and **without** a terminal
status (§7) is a transient drop. The edge keeps its local TCP open, reconnects,
and sends `Hello{Resume{inbound_delivered_through = <last written>}}` — a
reconnect always uses `Resume`, never `Open`, so a session the relay has already
reaped surfaces as a `NOT_FOUND` tombstone rather than silently re-materializing.
The handshake (6.1) re-establishes both resume points; retained frames are
replayed; dedupe absorbs any overlap. Neither DAP endpoint observes more than a
pause.

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

By contrast, a bare transport failure / `UNAVAILABLE` / `DEADLINE_EXCEEDED` is
**transient** → reconnect with `Resume` (§6.3).

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
- **Idle timeout** — no progress / both edges gone. (Configurable.)
- **Byte cap** — hard-coded maximum retained (unacked) bytes per session; exceed
  ⇒ abort with `RESOURCE_EXHAUSTED` (§5.4). Not configurable in the MVP.
- **Backpressure** — below the cap, HTTP/2 flow control pauses the source DAP
  endpoint rather than growing the buffer unbounded (§5.4).

The **detach-while-paused worker-slot hazard** (§5.4) is handled by the *worker*
terminating the action on a configurable timeout once the session ends; it is
not part of this protocol.

## 9. Deliberately out of scope (MVP)

- **Auth / authorization** (§5.6) — knowledge of the UUID is the only gate.
- **DAP awareness** — the relay never parses, validates, or transforms payloads.
- **Framing in the relay** — payloads are pre-framed discrete DAP messages.
- **Multiple sessions per action / multiple peers per side** — a second stream
  for a `{session_key, side}` is a takeover, not multiplexing.
- **Surviving a relay-process crash** — single non-replicated node (§5.2).

## 10. Open refinement questions

- **Piggybacked acks in the MVP** — include the field now (done) but defer
  actually emitting it? Or implement from the start?
- **`Close` detail fields** — do we want an optional action exit code / signal on
  `Close` for logging/telemetry, even though it is never turned into DAP?
- **Frontend↔relay hop** — confirmed as a transparent gRPC forward of the same
  proto (no internal proto). Validate this against the frontend's existing
  forwarding facilities.
- **Status-code choices** — confirm `NOT_FOUND` for tombstone vs.
  `FAILED_PRECONDITION`, and `ABORTED` vs. `CANCELLED` for reap/supersede.
- **Heartbeats** — rely on gRPC keepalive PINGs to detect half-open streams, or
  add an application-level heartbeat? (§5.4 leans on keepalive.)
