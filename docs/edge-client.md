# DAP Relay — Shared Edge-Client Behavior

> Status: **Draft for refinement.** Precursor to the forwarder spec (§7.4) and
> proxy spec (§7.5). It specifies the half of both edges that is identical: the
> reusable **edge-client** that speaks the relay protocol
> (`relay-protocol.md`). Section references like "§5.4" point at
> `high-level-design.md`; "protocol §X" points at `relay-protocol.md`.

## 1. Role and boundary

The proxy and forwarder differ only in what local socket they bridge and which
`side` they are. Everything protocol-facing is the same and lives in one
reusable **edge-client** (one library, two callers), as committed in §5.4 ("a
single relay-client library serves both the proxy and the forwarder").

An **edge-client instance bridges exactly one local connection to one relay
session.** It owns the relay-facing stream, sequencing, acks, retain/replay,
dedupe, reconnect, and termination. It does **not** know about DAP clients,
action ports, `bb_worker`, or CLIs.

| Concern | Edge-client (shared, here) | Edge layer (§7.4 / §7.5) |
|---|---|---|
| `Attach` stream, `Hello`, handshake | ✅ | declares `side`, `session_key`, relay endpoint |
| Outbound seq / framing / retain / replay | ✅ | — |
| Inbound dedupe / ordered write / delivery-ack | ✅ | — |
| Reconnect + resume, terminal-vs-transient | ✅ | — |
| LSP frame delimiting (read/write verbatim) | ✅ | — |
| gRPC keepalive | ✅ | — |
| Obtaining the local `net.Conn` | — | listens (proxy) / dials with retry (forwarder) |
| `CloseReason` + `exit_code` on local close | consumes | supplies |
| Running concurrent with `Runner.Run`, port discovery, CLI, UX | — | ✅ |

## 2. The local stream and framing

The edge-client reads and writes one local byte stream (a `net.Conn`) carrying
LSP-framed DAP messages. Per the verbatim decision (§5.4, protocol §2):

- **Reading (produce):** read the `Content-Length: <n>\r\n\r\n` header *only* to
  find the boundary, then take the **whole message verbatim — header bytes
  included — as the opaque payload** for one outbound `Frame`. Do not strip,
  normalize, or re-encode.
- **Writing (consume):** write an inbound payload to the local socket **byte for
  byte, unchanged**. No re-framing.

The edge-client never parses DAP JSON. A malformed/un-delimitable local frame is
a *local* protocol error (the local endpoint misbehaved): the edge-client tears
the session down as a local-close (§9.1) — it does not forward garbage.

## 3. Per-session state

For its one session the edge-client holds:

- **Outbound:** `next_seq` (starts at 1); a **retain buffer** of sent-but-unacked
  outbound `Frame`s in seq order.
- **Inbound:** `delivered_through` — the highest **contiguous** inbound seq it has
  **durably written** to the local socket (the high-water it announces on
  reconnect and acks to the relay).
- **Connection:** the current relay stream, and a flag marking any prior stream it
  has **superseded by its own reconnect** (for the `ABORTED` rule, §9.3).

Outbound state (`next_seq`, retain buffer) and `delivered_through` **survive
reconnects** — they live in the session, not the stream. They are lost only when
the edge-client instance itself ends (which means the local connection ended too;
see §9).

## 4. The bridge

Two pumps plus a connection manager, all sharing the §3 state:

- **Outbound pump:** local read → frame → assign seq → append to retain buffer →
  `Send(Frame)`.
- **Inbound pump:** `Recv` → (`Frame` | `Ack`) → on `Frame`: dedupe, write to
  local, advance `delivered_through`, send delivery-`Ack`; on `Ack` (receipt):
  drop retain buffer ≤ `through`.
- **Connection manager:** establishes/re-establishes the stream, runs the
  handshake, and on stream failure decides terminal-vs-transient (§9) — driving
  reconnect or shutdown.

## 5. Connect and handshake

On first connection the edge-client sends `Hello{session_key, side, Open}`; on
every reconnection it sends `Hello{session_key, side, Resume{delivered_through}}`
(protocol §6.1). It **never** uses `Open` to reconnect (protocol §6.3).

Then, per protocol §6.1:

1. The relay replies with `Ack{through = K}` — the relay's received high-water for
   this edge's **outbound** direction. The edge-client **drops retained outbound
   ≤ K** and (re)sends from `K + 1` in original seq order before sending any new
   outbound. (On a fresh `Open`, `K = 0`.)
2. The relay (re)starts delivering inbound from `delivered_through + 1`.

Both resume points are thus established before steady flow resumes.

## 6. Outbound path (produce)

For each whole DAP message read off the local socket:

1. Assign `seq = next_seq++`.
2. Build `Frame{seq, payload=<verbatim message>}`, append to the retain buffer.
3. `Send` it.

Retained frames are dropped only when a receipt-`Ack{through}` covers them
(`seq ≤ through`). Retained frames are **never reassigned a new seq**; a replay
re-sends them with their original seq (§5/§8). `Send` returning nil is *not*
delivery (§5.4) — only the receipt-ack releases the retain buffer.

## 7. Inbound path (consume)

For each inbound `Frame`:

1. **Dedupe:** if `seq ≤ delivered_through`, discard it (defensive backstop,
   §5.4) — it is a replayed duplicate.
2. Otherwise it is the next contiguous frame (the relay delivers in order from
   `delivered_through + 1`). If it is a `payload`, **write it verbatim** to the
   local socket; if it is a `Close`, handle per §9.2.
3. After the write **durably completes**, set `delivered_through = seq` and send a
   delivery-`Ack{through = delivered_through}` (cumulative; may be coalesced).

`delivered_through` advances **only on successful local write**, so it always
reflects bytes the local DAP endpoint has actually received — which is exactly
the resume/ack guarantee the relay relies on.

## 8. Reconnect and resume

A stream failure that is **transient** (§9.3) triggers reconnect: the edge-client
keeps the local socket open, dials a new `Attach`, and runs §5 with `Resume`.
Retained outbound frames are replayed from the relay's `Ack{through}`; inbound
resumes from `delivered_through + 1`; §7.1 dedupe absorbs any overlap. The local
DAP endpoint observes only a pause (§5.4). Reconnect uses backoff; there is no
fixed retry cap (the local socket staying open is what keeps the session alive —
the reconnect-grace/idle timers on the relay bound the other end).

## 9. Termination

The edge-client ends in exactly one of these ways. Three are terminal (stop, no
reconnect); the fourth is the transient path (§8).

### 9.1 Local close (this edge's socket ended)

Detected by a local read EOF/error or a local write failure (either direction
proves the socket dead; a malformed frame, §2, is treated the same). The
edge-client:

1. Emits a terminal `Frame{seq, Close{reason, exit_code, detail}}` as the **final
   outbound entry** (reason/exit_code supplied by the edge layer —
   `LOCAL_PEER_DISCONNECTED` for the proxy, `PROCESS_EXITED` + code for the
   forwarder).
2. Tries, **best-effort**, to get that `Close` **receipt-acked** by the relay (so
   the relay holds it to drain to the peer, protocol §7.1), including across a
   reconnect if the stream drops before the ack. This flush is **bounded by the
   edge layer**, not unbounded: the edge layer decides how long to keep trying
   before giving up (the proxy exits promptly; the forwarder's bound is part of
   the open coupling question, forwarder §8). If the flush is abandoned, the peer
   simply falls back to timeout-based teardown (protocol §8) instead of a clean
   `Close` — correctness holds, only the "clean end" UX degrades.
3. Then finishes. It does **not** keep delivering inbound — its local socket is
   gone, so inbound has nowhere to land; the relay drains this edge's `Close` to
   the peer and reaps.

### 9.2 Peer close (inbound `Close` received)

The peer's producer sent a terminal `Close` (protocol §7.1). The edge-client:

1. Has already written all preceding inbound payloads (they arrived in-order
   before the `Close`).
2. **Closes its local socket** (a plain FIN — the DAP endpoint sees a socket
   close; nothing is synthesized, §5.4).
3. Sends a delivery-`Ack` through the `Close`'s seq (lets the relay complete
   drain-then-drop) and finishes. **Does not reconnect.**

### 9.3 Relay-originated terminal status

The stream fails with `NOT_FOUND`, `RESOURCE_EXHAUSTED`, or `ABORTED` (protocol
§7.2): the edge-client closes its local socket and stops — **no reconnect**.

Everything else (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, connection reset, keepalive
timeout, …) is **transient** → §8.

**The `ABORTED` self-supersede rule:** if `ABORTED` arrives on a stream the
edge-client has **already replaced** by its own reconnect (the superseded flag,
§3), it is expected cleanup — ignore it. `ABORTED` on the **current/only** stream
is terminal (the session was reaped, or hijacked under the §5.6 no-auth gap). The
test is "do I have a newer stream?", not the code itself.

### 9.4 Terminal outcome reported to the edge layer

The edge-client returns a terminal outcome so the edge layer can react —
`LocalClosed`, `PeerClosed(reason, exit_code, detail)`, `Tombstone` (`NOT_FOUND`),
`Aborted`, or `CapExceeded` (`RESOURCE_EXHAUSTED`). (E.g. the forwarder logs the
peer's exit code; the proxy surfaces the end to its CLI/UX.)

## 10. Liveness

The edge-client configures gRPC keepalive PINGs on its side (protocol §8); a
keepalive timeout surfaces as a stream error and is treated as **transient** (§8).
No application-level heartbeat.

## 11. Interface to the edge layer

The edge-client is parameterized by, and only by:

- **Identity:** `session_key`, `side`.
- **Relay access:** how to dial a fresh `Attach` stream (frontend/relay endpoint,
  credentials, keepalive params).
- **Local connection:** the `net.Conn` to bridge (already accepted/dialed by the
  edge layer).
- **Local-close reason source:** a value/callback yielding the `CloseReason`
  (+ `exit_code`, `detail`) to stamp on the terminal `Close` when the local side
  ends — this is where the forwarder injects the action's exit status.

It exposes: **run until terminal**, returning the §9.4 outcome.

Everything else — listening vs. dialing, retry-dialing the action port, port
discovery, `BB_DEBUG_*` handling, the `Runner.Run` goroutine, CLI/UX — is the
edge layer's and is **out of scope here** (it belongs in §7.4 / §7.5).

## 12. Correctness invariants (checklist)

- Outbound seqs are dense and monotonic from 1; replays reuse original seqs.
- A frame leaves the retain buffer **only** on a covering receipt-ack.
- `delivered_through` advances **only** after a successful local write; it is the
  single source of truth for both the resume point and delivery-acks.
- Inbound is written to the local socket **at most once** (dedupe) and **in seq
  order**.
- Reconnect preserves all §3 state; only an instance end (local socket gone)
  discards it.
- A terminal `Close` is the **last** frame in its direction; nothing is produced
  after it.
- Reconnect is chosen **iff** the failure is transient (§9.3); the terminal set is
  never retried.

## 13. Open questions

- **Coalescing delivery-acks** — ack every frame, or batch (e.g. one ack per read
  batch / short timer)? Pure optimization; correctness holds either way since
  acks are cumulative. Default: cheap coalescing, ack at least whenever the
  inbound pump would otherwise block.
- **Reconnect backoff params** — initial/max/jitter; likely shared config with a
  sane default.
- **Local malformed-frame policy** — confirm "tear down as local-close" vs. a
  distinct diagnostic outcome.
