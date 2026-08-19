package io.github.richeyworks.smokesignal;

import io.github.richeyworks.smokehouse.SmokeHouse;
import io.github.richeyworks.smokehouse.SmokeHouseOptions;
import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
            }
        }
    }
}
