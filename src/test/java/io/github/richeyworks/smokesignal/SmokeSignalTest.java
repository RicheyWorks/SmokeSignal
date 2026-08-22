package io.github.richeyworks.smokesignal;

import io.github.richeyworks.smokehouse.SmokeHouse;
import io.github.richeyworks.smokehouse.SmokeHouseOptions;
import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wire against the oracle: every client op must behave byte-for-byte like calling the
 * store directly — including nulls, deletes of absent keys, and order statistics — across
 * one client and across two clients interleaving. Seeded and deterministic.
 */
class SmokeSignalTest {

    private static SmokeHouseOptions<Long, String> opts() {
        return SmokeHouseOptions.of(SpillSerializer.forLongs(), SpillSerializer.forStrings())
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
    }

    private static SmokeSignalClient<Long, String> client(int port) throws IOException {
        return SmokeSignalClient.connect(port, SpillSerializer.forLongs(),
                SpillSerializer.forStrings());
    }

    @Test
    void theWireBehavesExactlyLikeTheStore(@TempDir Path dir) throws IOException {
        Random rnd = new Random(42);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings());
             SmokeSignalClient<Long, String> wire = client(server.port())) {

            for (int i = 0; i < 600; i++) {
                long key = rnd.nextInt(150);
                if (rnd.nextInt(6) == 0) {
                    assertEquals(oracle.remove(key) != null, wire.delete(key), "delete " + key);
                } else {
                    String v = "v" + key + ":" + i;
                    wire.put(key, v);
                    oracle.put(key, v);
                }
                if (i % 50 == 0) {
                    long probe = rnd.nextInt(160);
                    assertEquals(oracle.get(probe), wire.get(probe), "get " + probe);
                }
            }
            assertEquals(oracle.size(), wire.size());
            assertEquals(oracle.subMap(20L, true, 90L, true).size(),
                    wire.countRange(20L, 90L), "order statistics over the wire");
            assertNull(wire.get(9_999L));
            assertFalse(wire.delete(9_999L));
        }
    }

    @Test
    void twoClientsInterleaveLikeTwoLocalThreads(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings());
             SmokeSignalClient<Long, String> a = client(server.port());
             SmokeSignalClient<Long, String> b = client(server.port())) {
            a.put(1L, "from-a");
            assertEquals("from-a", b.get(1L), "b sees a's write immediately");
            b.put(1L, "from-b");
            assertEquals("from-b", a.get(1L), "last writer wins across wires");
            assertTrue(a.delete(1L));
            assertNull(b.get(1L));
        }
    }

    @Test
    void aDeadClientNeverHurtsTheStore(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings())) {
            SmokeSignalClient<Long, String> doomed = client(server.port());
            doomed.put(7L, "seven");
            doomed.close();                                    // mid-session disconnect
            try (SmokeSignalClient<Long, String> fresh = client(server.port())) {
                assertEquals("seven", fresh.get(7L), "the store outlives its wires");
            }
        }
    }

    @Test
    void theWireCountsItsOwnTraffic(@TempDir Path dir) throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings())) {
            assertEquals(0, server.stats().requestsServed(), "a fresh wire has served nothing");

            try (SmokeSignalClient<Long, String> a = client(server.port());
                 SmokeSignalClient<Long, String> b = client(server.port())) {
                a.put(1L, "x");
                a.put(2L, "y");
                b.get(1L);
                b.delete(2L);
                a.size();
                a.countRange(0L, 10L);

                // Two clients connected; six well-formed requests answered, broken out by op.
                // Await the acceptor thread's connection count (it lands off the accept loop).
                long deadline = System.currentTimeMillis() + 5_000;
                while (server.stats().connectionsAccepted() < 2
                        && System.currentTimeMillis() < deadline) {
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                SmokeSignalServer.WireStats s = server.stats();
                assertEquals(2, s.connectionsAccepted(), "two clients connected");
                assertEquals(2, s.puts(), "two puts");
                assertEquals(1, s.gets(), "one get");
                assertEquals(1, s.deletes(), "one delete");
                assertEquals(1, s.sizeQueries(), "one size");
                assertEquals(1, s.rangeQueries(), "one range");
                assertEquals(6, s.requestsServed(), "six requests total");
                assertEquals(0, s.errors(), "no request was refused");
                assertTrue(s.line().contains("reqs=6"), "the readout line renders");
            }
        }
    }

    @Test
    void writesFollowTheRouteReadsStayOnTheStore(@TempDir Path dir) throws IOException {
        // The WriteRoute seam: every wire write must land through the route, never straight
        // on the served store — the routing rule an IndexedStore owner needs. The route here
        // tags values so a bypass would be visible in the bytes.
        java.util.concurrent.atomic.AtomicInteger routedPuts =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger routedDeletes =
                new java.util.concurrent.atomic.AtomicInteger();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            SmokeSignalServer.WriteRoute<Long, String> route =
                    new SmokeSignalServer.WriteRoute<>() {
                        @Override public void put(Long key, String value) throws IOException {
                            routedPuts.incrementAndGet();
                            store.put(key, "routed:" + value);
                        }
                        @Override public boolean delete(Long key) throws IOException {
                            routedDeletes.incrementAndGet();
                            return store.delete(key);
                        }
                    };
            try (SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store, route,
                         SpillSerializer.forLongs(), SpillSerializer.forStrings());
                 SmokeSignalClient<Long, String> wire = client(server.port())) {
                wire.put(1L, "a");
                assertEquals("routed:a", wire.get(1L), "the write went through the route");
                assertEquals("routed:a", store.get(1L), "and landed in the store the route chose");
                assertTrue(wire.delete(1L), "delete answers through the route");
                assertFalse(wire.delete(1L), "delete-of-absent is a no-op false, per the contract");
                assertEquals(1, routedPuts.get(), "exactly one routed put");
                assertEquals(2, routedDeletes.get(), "both deletes routed");
            }
        }
    }

    @Test
    void rangeAndStatsTravelTheWire(@TempDir Path dir) throws IOException {
        Random rnd = new Random(19);
        TreeMap<Long, String> oracle = new TreeMap<>();
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings());
             SmokeSignalClient<Long, String> wire = client(server.port())) {
            for (int i = 0; i < 300; i++) {
                long key = rnd.nextInt(150);
                String v = "v" + key + ":" + i;
                store.put(key, v);
                oracle.put(key, v);
            }

            // OP_RANGE: the fetched records equal the oracle's submap, in key order.
            var fetched = wire.rangeQuery(40L, 110L);
            assertEquals(oracle.subMap(40L, true, 110L, true),
                    new TreeMap<>(fetched), "the wire delivers the range exactly");
            assertEquals(new java.util.ArrayList<>(oracle.subMap(40L, true, 110L, true).keySet()),
                    new java.util.ArrayList<>(fetched.keySet()), "in key order");
            assertTrue(wire.rangeQuery(9_000L, 9_999L).isEmpty(), "an empty range is empty");

            // OP_STATS: the meter travels, and reading it is not itself metered.
            SmokeSignalServer.WireStats remote = wire.stats();
            assertEquals(server.stats(), remote, "the wire reports its own meter faithfully");
            assertEquals(2, remote.rangeQueries(), "both range fetches counted");
            SmokeSignalServer.WireStats again = wire.stats();
            assertEquals(remote.requestsServed(), again.requestsServed(),
                    "stats requests are observability, not traffic - unmetered");
        }
    }

    @Test
    void aBatchCrossesTheWireWholeAndLandsThroughTheBatchRoute(@TempDir Path dir)
            throws IOException {
        // The route receives the COMPLETE batch in one call, in staged order — the atomicity
        // decision belongs to the route, and the wire never applies half a request.
        java.util.List<Integer> batchSizes = java.util.Collections.synchronizedList(
                new java.util.ArrayList<>());
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            SmokeSignalServer.BatchRoute<Long, String> route = ops -> {
                batchSizes.add(ops.size());
                for (SmokeSignalServer.BatchOp<Long, String> op : ops) {
                    if (op.isPut()) {
                        store.put(op.key(), op.value());
                    } else {
                        store.delete(op.key());
                    }
                }
            };
            SmokeSignalServer.WriteRoute<Long, String> writes =
                    new SmokeSignalServer.WriteRoute<>() {
                        @Override public void put(Long k, String v) throws IOException { store.put(k, v); }
                        @Override public boolean delete(Long k) throws IOException { return store.delete(k); }
                    };
            try (SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store, writes,
                         route, SpillSerializer.forLongs(), SpillSerializer.forStrings());
                 SmokeSignalClient<Long, String> wire = client(server.port())) {
                int applied = wire.batch()
                        .put(1L, "a").put(2L, "b").put(3L, "c")
                        .delete(2L)
                        .commit();
                assertEquals(4, applied, "the server reports the whole batch applied");
                assertEquals(java.util.List.of(4), batchSizes,
                        "the route saw ONE call with all four ops");
                assertEquals("a", wire.get(1L));
                assertNull(wire.get(2L), "the staged delete landed in order");
                assertEquals("c", wire.get(3L));
                assertEquals(1, server.stats().batches(), "the batch is on the meter, once");
            }
        }
    }

    @Test
    void theDefaultBatchRouteAppliesInOrderThroughTheWriteRoute(@TempDir Path dir)
            throws IOException {
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings());
             SmokeSignalClient<Long, String> wire = client(server.port())) {
            wire.batch().put(10L, "x").put(10L, "y").delete(99L).commit();
            assertEquals("y", store.get(10L), "last writer wins inside the batch");
            assertEquals(1, store.size());
            // A committed batch cannot be reused.
            SmokeSignalClient<Long, String>.Batch b = wire.batch();
            b.put(1L, "once");
            b.commit();
            assertThrows(IllegalStateException.class, () -> b.put(2L, "again"),
                    "a committed batch refuses more staging");
        }
    }

    @Test
    void aHugeRefusalMessageDoesNotKillTheSession(@TempDir Path dir) throws IOException {
        // Tenth-pass S2w: a route throwing a >64KB message must not blow up writeUTF and drop
        // the connection — the refusal must come back and the session keep serving.
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts())) {
            String giant = "x".repeat(200_000);
            SmokeSignalServer.WriteRoute<Long, String> route =
                    new SmokeSignalServer.WriteRoute<>() {
                        @Override public void put(Long k, String v) {
                            throw new IllegalStateException(giant);   // an oversized message
                        }
                        @Override public boolean delete(Long k) throws IOException {
                            return store.delete(k);
                        }
                    };
            try (SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store, route,
                         SpillSerializer.forLongs(), SpillSerializer.forStrings());
                 SmokeSignalClient<Long, String> wire = client(server.port())) {
                IOException refused = assertThrows(IOException.class, () -> wire.put(1L, "a"),
                        "the oversized refusal comes back as an error, not a dropped connection");
                assertTrue(refused.getMessage().contains("truncated"), "and it was truncated");
                store.put(2L, "b");
                assertEquals("b", wire.get(2L), "the session keeps serving after the refusal");
            }
        }
    }

    @Test
    void anUnknownOpIsRefusedAndClosesTheSession(@TempDir Path dir) throws IOException {
        // ADR wire-framing (2026-08-20): an unknown op is a FRAMING error — the server cannot
        // know how many bytes the client intended to follow it, so it replies once and CLOSES
        // the session rather than parsing garbage. The store is untouched, and a FRESH client
        // still works (the server itself keeps accepting).
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings())) {
            store.put(5L, "five");

            try (java.net.Socket raw = new java.net.Socket(
                    java.net.InetAddress.getLoopbackAddress(), server.port())) {
                java.io.DataOutputStream out = new java.io.DataOutputStream(raw.getOutputStream());
                java.io.DataInputStream in = new java.io.DataInputStream(raw.getInputStream());
                out.writeByte(99);                             // not an op
                out.flush();
                assertEquals(2 /* REPLY_ERROR */, in.readByte(),
                        "an unknown op earns an error reply first");
                assertTrue(in.readUTF().contains("framing"), "and names the framing error");
                assertEquals(-1, in.read(), "then the session is closed (EOF), not left desynced");
            }

            assertEquals(1, server.stats().errors(), "the refusal is on the meter");
            assertEquals(0, server.stats().requestsServed(), "a refused op is not a served request");
            assertEquals("five", store.get(5L), "the store never felt it");

            // The SERVER keeps accepting — only the offending session died.
            try (SmokeSignalClient<Long, String> fresh = client(server.port())) {
                assertEquals("five", fresh.get(5L), "the server keeps serving new clients");
            }
        }
    }

    @Test
    void anUnknownBatchOpTypeClosesInsteadOfCorruptingTheStore(@TempDir Path dir)
            throws IOException {
        // The desync bug S1w named: a bad op-type byte mid-batch used to be reinterpreted as
        // fresh opcodes, writing garbage into the store. Now it's a framing error — the store
        // is untouched and the session closes.
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings())) {
            try (java.net.Socket raw = new java.net.Socket(
                    java.net.InetAddress.getLoopbackAddress(), server.port())) {
                java.io.DataOutputStream out = new java.io.DataOutputStream(raw.getOutputStream());
                java.io.DataInputStream in = new java.io.DataInputStream(raw.getInputStream());
                out.writeByte(6);                              // OP_BATCH
                out.writeInt(2);                               // claims 2 ops
                out.writeByte(1);                              // op 1: PUT
                SpillSerializer.forLongs().write(1L, out);
                SpillSerializer.forStrings().write("one", out);
                out.writeByte(77);                             // op 2: a bogus op-type byte
                SpillSerializer.forLongs().write(2L, out);
                out.flush();
                assertEquals(2 /* REPLY_ERROR */, in.readByte(), "the bad batch op is refused");
                in.readUTF();
                assertEquals(-1, in.read(), "and the session closes rather than parsing garbage");
            }
            assertEquals(0, store.size(), "no half-batch, no garbage — the store is untouched");
        }
    }

    @Test
    void orderStatisticsOverTheWireMatchTheStore(@TempDir Path dir) throws IOException {
        // Additive slice (2026-08-21): the store's order-statistics surface — rank, nth, median —
        // reachable over the wire. The wire's contract is unchanged: every op behaves exactly like
        // calling the store directly, checked here against the store as its own oracle.
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings());
             SmokeSignalClient<Long, String> wire = client(server.port())) {

            // Empty store: median is null, any rank is out of range (a clean error), absent key ranks 0.
            assertNull(wire.medianKey(), "median of an empty store is null, over the wire");
            assertThrows(IOException.class, () -> wire.nthKey(1), "nth of an empty store is out of range");
            assertEquals(0, wire.rankOf(5L), "rank of an absent key is 0");

            // Odd keys 1,3,...,199 — 100 keys, so ranks are well spread and boundaries are exact.
            for (long k = 1; k < 200; k += 2) {
                wire.put(k, "v" + k);
            }
            int n = store.size();
            assertEquals(100, n);

            // rankOf: present and absent keys alike agree with the store.
            for (long k = 0; k <= 200; k++) {
                assertEquals(store.rankOf(k), wire.rankOf(k), "rankOf " + k);
            }
            // nthKey: every in-range rank agrees; the two out-of-range ends are a clean wire error.
            for (int r = 1; r <= n; r++) {
                assertEquals(store.nthKey(r), wire.nthKey(r), "nthKey " + r);
            }
            assertThrows(IOException.class, () -> wire.nthKey(0), "rank 0 is out of range");
            assertThrows(IOException.class, () -> wire.nthKey(n + 1), "rank size+1 is out of range");
            assertEquals(store.medianKey(), wire.medianKey(), "median over the wire matches the store");

            // The session survives the out-of-range errors: ordinary ops still work (WP-1 alignment).
            assertEquals("v99", wire.get(99L), "the session stays aligned after order-stat errors");
            assertEquals(100, wire.size());
        }
    }

    @Test
    void firstLastAndPercentileOverTheWireMatchTheStore(@TempDir Path dir) throws IOException {
        // Additive slice (2026-08-21), completing the wire's order-statistics surface: first, last,
        // and percentile keys. All three answer null on an empty store; percentile clamps its pct to
        // [1,100] rather than throwing, so every int answers a key on a non-empty store.
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts());
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings());
             SmokeSignalClient<Long, String> wire = client(server.port())) {

            assertNull(wire.firstKey(), "first of an empty store is null");
            assertNull(wire.lastKey(), "last of an empty store is null");
            assertNull(wire.percentileKey(50), "percentile of an empty store is null");

            for (long k = 1; k <= 100; k++) {
                wire.put(k, "v" + k);
            }
            assertEquals(store.firstKey(), wire.firstKey(), "firstKey over the wire matches the store");
            assertEquals(store.lastKey(), wire.lastKey(), "lastKey matches");
            for (int p : new int[]{1, 25, 50, 75, 100}) {
                assertEquals(store.percentileKey(p), wire.percentileKey(p), "percentile " + p);
            }
            // Out-of-range percentiles clamp (they answer a key, not an error) — exactly as the store.
            assertEquals(store.percentileKey(0), wire.percentileKey(0), "pct 0 clamps to the minimum");
            assertEquals(store.percentileKey(101), wire.percentileKey(101), "pct 101 clamps to the maximum");

            assertEquals("v50", wire.get(50L), "the session stays aligned");
        }
    }

    @Test
    void aRefusedCountRangeKeepsTheSessionAligned(@TempDir Path dir) throws IOException {
        // Twelfth pass: OP_COUNT_RANGE must compute the count BEFORE it writes REPLY_VALUE (the S1w
        // "materialize before write" discipline OP_RANGE already keeps). execute() turns a thrown
        // RuntimeException into a REPLY_ERROR but cannot un-write bytes already sent, so writing
        // REPLY_VALUE first and then throwing lands the error AFTER it — desyncing every later
        // request. A comparator that rejects a poison key makes countRange throw, exercising exactly
        // that path.
        Comparator<Long> poison = (a, b) -> {
            if (a == -999L || b == -999L) {
                throw new RuntimeException("incomparable poison key");
            }
            return Long.compare(a, b);
        };
        SmokeHouseOptions<Long, String> opts = SmokeHouseOptions.of(
                        SpillSerializer.forLongs(), SpillSerializer.forStrings(), poison)
                .indexTier(SmokeHouseOptions.IndexTier.STATIC);
        try (SmokeHouse<Long, String> store = SmokeHouse.open(dir, opts);
             SmokeSignalServer<Long, String> server = SmokeSignalServer.serve(store,
                     SpillSerializer.forLongs(), SpillSerializer.forStrings());
             SmokeSignalClient<Long, String> wire = client(server.port())) {
            wire.put(1L, "one");
            wire.put(2L, "two");
            assertEquals(2, wire.size(), "the wire serves before the poison");

            // The count runs the store's comparator against the poison key, which throws. It must
            // come back as a clean wire error — not a REPLY_VALUE followed by an error that desyncs.
            assertThrows(IOException.class, () -> wire.countRange(-999L, -999L),
                    "a comparator that throws surfaces as a recoverable wire error");

            // The session must still be aligned: every later request behaves normally.
            assertEquals("one", wire.get(1L), "the session survived the refused count");
            assertEquals(2, wire.size(), "and keeps serving reads");
            wire.put(3L, "three");
            assertEquals("three", wire.get(3L), "and writes");
        }
    }
}
