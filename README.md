# SmokeSignal

[![CI](https://github.com/RicheyWorks/SmokeSignal/actions/workflows/ci.yml/badge.svg)](https://github.com/RicheyWorks/SmokeSignal/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Java 17](https://img.shields.io/badge/Java-17-orange.svg)](https://adoptium.net/)

Engine ten of the ecosystem: **the wire** — a zero-dependency, loopback-only binary protocol
putting a SmokeHouse on a JDK socket. GET / PUT / DELETE / SIZE / COUNT_RANGE, framed by the
store's own `SpillSerializer`s (the same framing replication rides).

```java
try (var server = SmokeSignalServer.serve(store, keySer, valSer);
     var wire   = SmokeSignalClient.connect(server.port(), keySer, valSer)) {
    wire.put(42L, "brisket");
    wire.get(42L);
    wire.countRange(10L, 99L);     // order statistics over the wire
}
```

Said loudly: this is an **in-machine protocol face, not a network service** — loopback only,
no auth, no TLS, no remote binding. N clients interleave exactly like N local threads (every
op lands on the store's single writer lock). A dead wire never hurts the store.

## Design notes

- **Loopback only, and it says so.** No auth, no TLS, no remote binding — an in-machine
  protocol face, not a network service. Remote access would be a different engine with an
  auth/transport ADR behind it.
- **The framing is the store's own.** Keys and values cross the wire through
  `SpillSerializer`s — self-delimiting, the same contract replication rides. Op codes are
  the protocol: add new ones, never repurpose old ones.
- **All consistency is the store's.** Every op lands on the single writer lock, so N wires
  interleave exactly like N local threads; the server adds no caching, no queueing, no
  reordering. Bad requests get `REPLY_ERROR`; a dead wire ends its session and nothing else.
- **Writes take the route (2026-08-19).** `serve(store, writeRoute, …)` sends every wire
  PUT/DELETE through a caller-supplied `WriteRoute` while reads stay on the served store —
  the seam a composed caller (an `IndexedStore` owner) needs so wire writes reach the index
  fan-out instead of bypassing it. The plain `serve(store, …)` overload keeps the old
  behavior: the route *is* the store. Same lesson Twine's sink seam taught, pointed at the
  wire; WholeHog's findings ledger carried it as "deliberately unsolved" until this consumer
  arrived.
- **The range travels, and so does the meter (2026-08-20).** `OP_RANGE` delivers a key
  range's records in key order — `countRange` could always count them; `rangeQuery(lo, hi)`
  fetches them (materialized on both ends: ask for sane ranges, the honesty countRange
  always demanded). `OP_STATS` sends the server's own `WireStats` to any client —
  observability reaching the wire's far end — and reading the meter is deliberately not
  metered.
- **Batches cross whole (2026-08-19).** `OP_BATCH` carries a staged list of puts/deletes as
  one request; the server reads the entire batch off the socket before touching its
  `BatchRoute`, so the route decides atomicity and the wire never applies half a request.
  The default route applies in order through the `WriteRoute` (sequential, honestly not
  atomic); a caller with a real atomic batcher — Twine is the ecosystem's — supplies its own
  and gives every wire client crash-atomic multi-key batches. Client side:
  `wire.batch().put(k, v).delete(k2).commit()`.

## The ecosystem

Eleven engines, one organism — each in its own repo, composed by nested Gradle
composite builds:

| Engine | Role |
|---|---|
| [CSRBT](https://github.com/RicheyWorks/CSRBT) | the adaptive ordered index — orders the world |
| [SuperBeefSort](https://github.com/RicheyWorks/SuperBeefSort) | the intake tract — profiles, sorts, feeds in O(n) |
| [SmokeHouse](https://github.com/RicheyWorks/SmokeHouse) | the log-structured store — durability, tail, watchers, replicas |
| [Carver](https://github.com/RicheyWorks/Carver) | the read planner — decides how to read |
| [Renderer](https://github.com/RicheyWorks/Renderer) | the materialized-view engine — folds the tail into live aggregates |
| [Brine](https://github.com/RicheyWorks/Brine) | the adaptive cache — eviction policy evolved per workload |
| [PitBoss](https://github.com/RicheyWorks/PitBoss) | the fleet conductor — lag watch, re-bootstrap, the promotion runbook |
| [DryAge](https://github.com/RicheyWorks/DryAge) | the time-travel engine — as-of reads over preserved history |
| [Twine](https://github.com/RicheyWorks/Twine) | crash-atomic multi-key batches — journaled commit, idempotent replay |
| **SmokeSignal** (this repo) | the wire — a loopback protocol face for the store |
| [Jerky](https://github.com/RicheyWorks/Jerky) | cold storage — compressed, CRC-verified backup archives |
| [WholeHog](https://github.com/RicheyWorks/WholeHog) | the integration organism — all of them, at once |
| [Rub](https://github.com/RicheyWorks/Rub) | the observability engine — tail meter + store gauge, fused into vitals |
| [Sizzle](https://github.com/RicheyWorks/Sizzle) | the chaos engine — deterministic fault injection at the write seam |

## Build

```bash
# Requires ../SmokeHouse, ../SuperBeefSort, ../CSRBT cloned as siblings (nested composite build)
./gradlew build
```

Java 17+, Gradle 9.5.1 (bundled wrapper). Seeded oracle tests in the house style. MIT license.
