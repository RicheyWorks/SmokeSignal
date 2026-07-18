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

## The ecosystem

Engines 1–6: [CSRBT](https://github.com/RicheyWorks/CSRBT) (index) · [SuperBeefSort](https://github.com/RicheyWorks/SuperBeefSort) (intake) · [SmokeHouse](https://github.com/RicheyWorks/SmokeHouse) (store) · [Carver](https://github.com/RicheyWorks/Carver) (read planner) · [Renderer](https://github.com/RicheyWorks/Renderer) (materialized views) · [Brine](https://github.com/RicheyWorks/Brine) (adaptive cache).
Engines 7–11: [PitBoss](https://github.com/RicheyWorks/PitBoss) (fleet conductor) · [DryAge](https://github.com/RicheyWorks/DryAge) (time travel) · [Twine](https://github.com/RicheyWorks/Twine) (atomic batches) · [SmokeSignal](https://github.com/RicheyWorks/SmokeSignal) (the wire) · [Jerky](https://github.com/RicheyWorks/Jerky) (cold archives).

## Build

```bash
# Requires ../SmokeHouse, ../SuperBeefSort, ../CSRBT cloned as siblings (nested composite build)
./gradlew build
```

Java 17+, Gradle 9.5.1 (bundled wrapper). Seeded oracle tests in the house style. MIT license.
