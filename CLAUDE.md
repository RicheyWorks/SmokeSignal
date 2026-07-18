# SmokeSignal — working notes for agents

Engine 10: loopback wire protocol. Two classes: `SmokeSignalServer` (accept loop + per-
session daemon threads, op dispatch) and `SmokeSignalClient` (synchronous request/reply,
one lock per client). Framing = `SpillSerializer`s, self-delimiting.

## Invariants (do not break)
- **Loopback only.** Never bind beyond `InetAddress.getLoopbackAddress()`; remote access
  is a different engine with an auth/TLS ADR behind it.
- **The wire is stateless per op**; all consistency is the store's. Bad requests reply
  REPLY_ERROR; a dead client just ends its session.
- Protocol changes bump nothing silently — op codes are the contract; add, never repurpose.
- Oracle tests in `SmokeSignalTest` (wire ≡ direct store calls).

## Git is host-side
Same as the siblings: agent sandboxes cannot write `.git`. Run all git commands from a host
terminal (PowerShell). Stale `.git/index.lock` fix: `Remove-Item .git\index.lock -Force`.
