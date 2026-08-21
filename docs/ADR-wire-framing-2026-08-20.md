# ADR: wire framing and error recovery — 2026-08-20

**Status: Accepted.** Fired by tenth-pass findings S1w/S3w: an error thrown after a request's
framed fields had begun to be read left the socket at an unknown position, and the old server
loop wrote `REPLY_ERROR` and *kept parsing* — reinterpreting the rest of the request's bytes as
opcodes and, in the worst case, writing garbage into the store. The client had the mirror bug:
a serializer throwing mid-request left a partial frame buffered, which the next call flushed.

## The context

SmokeSignal is a length-free binary protocol: an op byte, then serializer-framed fields. That
framing is self-delimiting *when every field is read successfully*, but it carries no
per-request length prefix — so once a read fails partway, the server cannot know where the next
request begins. Recovery is impossible without either (a) length-prefixing every request, or
(b) ending the session on any framing error. This is fundamental to a length-free protocol; it
is not a bug that a bigger try/catch fixes.

## The decision

**Distinguish two failure classes, because they have opposite safe responses:**

1. **Framing error** — a failure *while reading* a request's fields, or an unknown op / unknown
   batch-op byte. The stream position is now unknown. The server replies `REPLY_ERROR` once and
   **closes the session**. Continuing would corrupt.
2. **Execution error** — a failure *after* the whole request was read, while acting on it (a
   store refusal, a route throwing, a bad-argument `RuntimeException`). The stream is aligned;
   the reply slot is the server's. The server writes `REPLY_ERROR` and **keeps serving**.

Concretely: each op handler reads all its fields first (the read phase); a throw there
propagates to the session loop, which closes. Only then does it touch the store/route and write
the reply (the execute phase), wrapped so a `RuntimeException` becomes a recoverable
`REPLY_ERROR`. `IOException` from the store or the socket ends the session as it always did (a
broken store or a dead wire is not a bad request).

We chose **close-on-framing-error over length-prefixing**: length prefixes would let a client
recover after a malformed request, but they cost a size int on every request forever and invite
the very trust-the-count allocation bug S4w just fixed. A loopback in-machine protocol does not
need mid-stream resync; a client that sent a malformed request has a bug, and dropping its
session is the honest signal.

**Client side (S3w):** each request is serialized into an in-memory buffer and written to the
socket only when complete. A serializer throwing mid-request now sends *nothing* — the client
stays aligned and usable, instead of leaking a partial frame that the next call would flush.

## Consequence for existing behavior

The previous "an unknown op is refused and the session keeps serving" behavior was only ever
safe because the test client sent nothing after the bad byte; in general an unknown op is an
unparseable stream. Under this ADR the server replies error and closes on an unknown op — the
`anUnknownOpIsRefusedCountedAndHarmless` test is updated to assert the honest contract (error,
then the connection ends). Execution-error recovery — the valuable case, e.g. the oversized-
refusal test from S2w — is unchanged and still keeps the session alive.
