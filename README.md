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

## Build

```bash
# Requires ../SmokeHouse, ../SuperBeefSort, ../CSRBT cloned as siblings (nested composite build)
./gradlew build
```

Java 17+, Gradle 9.5.1 (bundled wrapper). Seeded oracle tests in the house style. MIT license.
