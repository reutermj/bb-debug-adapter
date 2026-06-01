# DAP Relay — Server Behavior (`bb_dap_relay`)

> Status: **Draft for refinement.** Companion to [`relay-protocol.md`](relay-protocol.md)
> (the wire spec). That document defines the grammar an edge and the relay speak;
> this one specifies the **server side** — the per-session state `bb_dap_relay`
> holds, how sessions materialize, how frames are buffered/forwarded/GC'd, how
> resume and termination are handled, and the reaping/config surface. Section
> references: "§5.4" → [`high-level-design.md`](high-level-design.md); "protocol
> §X" → `relay-protocol.md`; "edge-client §X" → [`edge-client.md`](edge-client.md).

## 1. Role and scope

`bb_dap_relay` is the **single backend node** (§5.2) that implements the
`DebugAdapterRelay.Attach` RPC (protocol §5). It is the **custodian of per-session
state** — the only stateful new component in the design.

- **DAP-agnostic.** Every payload is opaque bytes; the relay never parses,
  validates, or transforms DAP (§5.4).
- **Reached two ways, treated one way.** The developer proxy arrives via the
  `bb_storage` frontend's transparent forward; the worker forwarder dials the
  relay directly (§4, §5.2). To the relay both are simply `Attach` streams — it
  does **not** distinguish, or care, how a stream arrived.
- **Single, non-replicated.** All state for a session lives in one process
  (§5.2). There is no shared store and no cross-replica affinity.

**Out of scope for the MVP:** observability / metrics / structured logging
(**punted to after a functional MVP**), authorization (§5.6), multi-replica
horizontal scaling (§5.2), DAP awareness, and durability across a relay-process
crash (§12).

## 2. State model

The relay holds a process-wide map `session_key → Session`, populated lazily
(§3). A `Session` owns two independent **Directions** and the attachment slots
for the two **Sides**.

**Directions** (protocol §4.1). Each is an ordered log with a single producer and
single consumer, fixed by the direction:

| Direction | Producer side | Consumer side |
|---|---|---|
| `C2S` | `PROXY` | `FORWARDER` |
| `S2C` | `FORWARDER` | `PROXY` |

Per Direction the relay tracks:

- **`received_high_water`** — the highest `seq` taken into custody from the
  producer, contiguous from 1.
- **`delivered_ack`** — the highest `seq` the consumer has delivery-acked; the GC
  point for this direction.
- **`retained`** — the in-flight window `(delivered_ack, received_high_water]`, in
  `seq` order: frames received but not yet delivery-acked by the consumer. This is
  what the byte cap bounds (§6).
- **`close`** — the terminal `Close` marker, if the producer has closed, ordered
  **after** the last payload (protocol §7.1).

**Sides.** For each of `PROXY` and `FORWARDER` the `Session` tracks the
currently-attached stream (or none) and its liveness, plus the timestamps the
reaper uses (materialized-at, last-disconnect). A side's **outbound** direction is
the one it produces and its **inbound** the one it consumes (protocol §4.2).

**Invariant — state lives in the `Session`, not the stream.** All direction state
(`received_high_water`, `delivered_ack`, `retained`, `close`) survives stream
drops and takeovers. It is discarded **only** when the session is reaped (§8–§9).

## 3. Materialization — lazy first-touch (§5.4)

On each `Attach`, the relay reads the first message (`Hello`) and resolves the
session by `session_key` and the `attach` variant (protocol §6.1):

- **`Open`, session absent** → **create** the `Session` (allocate the two empty
  Directions), then attach this side. **This is the only path that creates a
  session.**
- **`Open` or `Resume`, session present** → attach this side (§4).
- **`Resume`, session absent** → fail the stream with **`NOT_FOUND`** (the
  tombstone, protocol §7.3). A `Resume` **never** materializes.

There is no `CreateSession` RPC and no designated creator: either side may arrive
first, and the second attaches to the already-materialized session. The relay
keeps **no tombstone records** — "absent" simply means "no live session," and only
`Open` may create one (protocol §7.3).

## 4. Handshake (relay side of protocol §6.1)

When a stream successfully attaches for `{session_key, side}`:

### 4.1 Takeover / supersede

If another stream is **already attached** for the same `{session_key, side}`, the
**new stream takes over**: the relay closes the old stream with **`ABORTED`**
("superseded") and installs the new one. The old stream's pump stops; **direction
state is untouched** (it is in the `Session`, §2). This single mechanism serves
both a normal reconnect and the §5.6 no-auth hijack — the relay does not
distinguish them.

### 4.2 Exchange both resume points

1. The relay sends **`Ack{through = received_high_water}` of this side's
   *outbound* direction** as the first `RelayToEdge` message. This is the
   producer's resume point: drop retained outbound `≤ through`, resend from
   `through + 1`. On a freshly-created session it is `0`.
2. The relay (re)starts delivering this side's *inbound* direction from
   **`inbound_delivered_through + 1`** — `1` for an `Open`, or the value the edge
   announced in `Resume`.

**Receiver-owned resume.** The relay replays from the **consumer's announced
high-water**, never from its own `delivered_ack` (which can lag acks that were in
flight when the stream dropped, protocol §6.1/§4.3). Its `delivered_ack` drives
**GC only** (§6).

## 5. Steady-state forwarding

For each attached stream the relay runs two concerns concurrently.

**Receiving the producer's outbound direction:**

1. `Recv` a `Frame{seq, payload|close}`.
2. **Defensive dedupe.** If `seq ≤ received_high_water`, it is a replayed
   duplicate (a producer that resent across reconnect) — ignore it, but still
   receipt-ack. Otherwise it is `received_high_water + 1` (producers send densely,
   in order, protocol §6.2); take it into custody (append to `retained`) and
   advance `received_high_water`.
3. Send **receipt-`Ack{through = received_high_water}`** to the producer so it may
   drop its retain copy (protocol §4.3). May be coalesced.
4. Make it available to the consumer (below).

A `Frame.piggyback_ack_through` is ignored for the MVP (protocol §6.2). An `Ack`
arriving from this stream is a **delivery-ack for the direction this side
*consumes*** → GC that direction (§6).

**Delivering a direction to its consumer:**

- The relay sends `retained` frames **in `seq` order** to the attached consumer
  stream, beginning at the consumer's resume point (§4.2) and continuing as new
  frames arrive.
- It does **not** block on per-frame delivery-acks — acks are cumulative and drive
  GC, not flow (§6). Ordering is preserved.
- If **no consumer is attached**, frames simply accrue in `retained`
  (store-and-forward, §5.4) until a consumer attaches or a timer reaps the session
  (§9). Neither side has to be present for the other to make progress.

## 6. GC and buffer bounds (§5.4)

- **Delivery-ack drives the relay's GC.** On `Ack{through}` from a direction's
  consumer, drop `retained` frames with `seq ≤ through` and advance
  `delivered_ack`. Cumulative.
- **Receipt-ack drives the *producer's* drop, not the relay's.** The relay keeps a
  frame until it is **delivery**-acked — `Send()` to the consumer is not delivery
  (protocol §4.3). Custody only transfers when the consumer confirms it wrote the
  frame to its local socket.
- **Byte cap — hard-coded, per session, abort on exceed.** The relay bounds the
  total `retained` bytes across both directions of a session by a hard-coded
  constant. On exceed it **aborts the whole session** with `RESOURCE_EXHAUSTED`
  (protocol §7.2/§8). Evicting an unacked frame would corrupt the DAP stream, so
  the cap must abort, not drop (§5.4). At debug volume this is a pathological-case
  safety valve that should never fire.

**Backpressure — the cap is the bound; flow control is best-effort.** high-level
§5.4 describes HTTP/2 flow control "propagating backpressure to the source DAP
endpoint."
That holds only loosely once the relay is store-and-forwarding, because to buffer
for an absent or slow peer the relay must **decouple** the two legs — and each
edge's frames *and* its delivery-acks ride the **same** bidi stream, so a relay
that paused reading a stream to push back would also stall the **reverse**
direction's acks (wedging that direction's GC). Therefore, for the MVP:

- The relay **continuously services `Recv` on both streams** so cumulative acks
  always flow.
- The **byte cap is the authoritative memory bound**; flow control is a secondary
  effect (when the relay's own `Send` to a slow consumer blocks, it naturally
  stops forwarding that direction, and the producer is bounded by the cap).
- A relay **may** pause reading a producer at a soft high-watermark to push back on
  a live-but-fast producer, accepting that this briefly delays the reverse
  direction's acks (harmless at debug volume). **Not required** for the MVP.

## 7. Reconnect and resume (relay side)

- **Producer reconnect** (`Resume`): the relay re-emits `Ack{received_high_water}`
  (§4.2); the producer resends from `through + 1`; the §5 defensive dedupe absorbs
  any `≤ received_high_water` overlap.
- **Consumer reconnect** (`Resume{inbound_delivered_through}`): the relay replays
  `retained` from `inbound_delivered_through + 1` in `seq` order; the consuming
  edge dedupes defensively (edge-client §7).
- **Takeover**: a second stream for a side supersedes the first (§4.1) and
  continues from `Session` state.
- The relay never loses direction state across a reconnect; only a reap discards
  it (§8–§9).

## 8. Termination

### 8.1 Graceful — drain-then-drop (protocol §7.1)

A producer's terminal `Close` is the **final `Frame`** in its direction, ordered
after the last payload. The relay treats it like any frame: buffer it, deliver it
in order. The relay **reaps the session only after** the consumer's delivery-ack
covers the `Close` seq (drain complete), or a timer fires (§9). This protects the
final DAP `terminated`/`exited` payloads, consistent with "evicting an unacked
message corrupts the session" (§5.4).

**Asymmetry on `PROCESS_EXITED`.** When the forwarder closes `S2C` (the action /
DAP server exited), new `C2S` frames have nowhere to go and are **refused /
discarded**, while buffered `S2C` frames must still **drain to the proxy**
(protocol §7.1).

Teardown is **idempotent**: both directions may close, and the relay reaps once
both are drained/terminal (or a timer fires).

### 8.2 Relay-originated abrupt (protocol §7.2)

The relay fails a stream with a gRPC status (no drain) in these cases:

| Condition | Status |
|---|---|
| `Resume` on absent/reaped session | `NOT_FOUND` (tombstone) |
| Byte cap exceeded | `RESOURCE_EXHAUSTED` |
| Idle / orphan / grace reap while a stream is connected | `ABORTED` |
| Superseded by a newer stream | `ABORTED` ("superseded") |

On reap, the relay closes the **surviving** peer's stream with the appropriate
status so its edge stops reconnecting and propagates the end downward (§5.4). Reap
is idempotent (both edges may signal end concurrently).

## 9. Timers and reaping (protocol §8, §5.4)

Three timers plus the byte cap bound a session's lifetime. A dropped gRPC stream
is **never** by itself a teardown — it is ambiguous (crash vs. mid-reconnect), so
the relay distinguishes a transient relay-hop disconnect (retain, await replay)
from a terminal end (an in-band `Close`, §8.1).

- **Reconnect-grace** — after a stream drops with **no** in-band `Close` and **no**
  terminal status, retain the session awaiting reconnect; reap on expiry.
- **Orphan** — materialized, but the second side never attaches; reap on expiry.
- **Idle** — fires **only** on a session whose edges are **disconnected** (no live
  stream / no keepalive). A developer paused at a breakpoint with both streams up
  and heartbeating is fully live and is **not** reaped, however long the quiet
  lasts. Idleness is measured by **absence of connections**, never by absence of
  DAP traffic.

**Liveness — gRPC keepalive PINGs, no app heartbeat (§8).** A half-open stream
surfaces as a stream error, which is what flips a direction from *deliver* to
*retain-for-replay* and starts the reconnect-grace timer. The relay's server-side
keepalive **enforcement policy must permit** the edges' client keepalives, and the
keepalive timeout should be comfortably shorter than the grace/idle timeouts so a
dead stream is noticed well before a session would be reaped.

## 10. Concurrency model (informative)

The relay's internal structure is an implementation choice; what is **load-bearing**
is the set of invariants it must preserve. A natural shape: a per-session guard (a
lock or a single owning goroutine) over the two Directions, a recv/send pump per
attached stream, and a reaper watching the timers.

Required invariants:

- `received_high_water` advances **only** on contiguous, in-order custody.
- A `retained` frame leaves **only** on a covering delivery-ack (§6) or a
  whole-session reap.
- Delivery to a consumer is **in `seq` order** and resumes from the consumer's
  announced high-water on each (re)attach.
- A takeover installs the new stream and stops the old **without touching**
  direction state (§4.1).
- Reap is **idempotent** and closes the surviving peer.

## 11. Configuration

- **Timeout values** — reconnect-grace, orphan, and idle are exposed in the normal
  Buildbarn jsonnet configuration; sensible defaults ship, operators tune them
  (§5.4). *(Default values still open, §14.)*
- **Byte cap** — a **hard-coded constant** for the MVP; not configurable (§6, §8).
- **gRPC server** — listen endpoint, TLS, and a keepalive enforcement policy that
  permits the edges' client keepalives (§9).
- **No auth config** — the MVP gates only on the unguessable `session_key` (§5.6).

## 12. Crash / durability

A single, non-replicated node means a **relay-process crash ends all live
sessions** (protocol §4.3). After a crash the relay has no record of any session,
so an edge's `Resume` returns `NOT_FOUND` → tombstone → the edge closes its local
socket. This is an accepted MVP limitation: disconnect tolerance covers stream
drops (network blips, frontend restarts), **not** relay-process loss (§5.2).

## 13. Out of scope (MVP)

- **Observability / metrics / structured logging** — punted to after a functional
  MVP.
- **Auth** (§5.6), **multi-replica / horizontal scaling** (§5.2), **DAP
  awareness**, **durability across a crash** (§12).
- **Defensive handling of a misused `Open`** (protocol §6.3/§10) — the relay
  trusts the edges (both our code) to reconnect with `Resume`; it does not reject
  or normalize an `Open` for a side that already has delivery progress.

## 14. Open questions

- **Timer default values** — concrete defaults for reconnect-grace, orphan, and
  idle (shared with §5.4's open values).
- **Read-pause backpressure** (§6) — whether to implement the soft-high-watermark
  read-pause or rely solely on the byte cap. MVP leans on the cap; the pause is an
  optional refinement.
- **`C2S`-after-forwarder-close** (§8.1) — whether to discard such frames silently
  or surface anything to the producing edge (which will see its stream closed on
  reap regardless).
