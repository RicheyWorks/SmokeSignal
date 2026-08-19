package io.github.richeyworks.smokesignal;

import io.github.richeyworks.smokehouse.SmokeHouse;
import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SmokeSignal — engine ten of the ecosystem: the wire. A zero-dependency, loopback-only
 * binary protocol putting a SmokeHouse on a JDK socket: GET / PUT / DELETE / SIZE /
 * COUNT_RANGE, keys and values framed by the store's own {@code SpillSerializer}s — the same
 * framing replication rides. The seventh-engine ADR deferred this behind the Central release;
 * that deferral was overridden by decree (2026-07-18), with the loopback constraint kept:
 * <b>this is an in-machine protocol face, not a network service</b> — no auth, no TLS, no
 * remote binding, and it says so loudly rather than pretending otherwise.
 *
 * <p>Concurrency is the store's own: every client op lands on the store's single writer lock,
 * so N clients interleave exactly like N threads calling the store directly.</p>
 */
public final class SmokeSignalServer<K, V> implements Closeable {

    static final byte OP_GET = 1;
    static final byte OP_PUT = 2;
    static final byte OP_DELETE = 3;
    static final byte OP_SIZE = 4;
    static final byte OP_COUNT_RANGE = 5;
    static final byte REPLY_NULL = 0;
    static final byte REPLY_VALUE = 1;
    static final byte REPLY_ERROR = 2;

    private final SmokeHouse<K, V> store;
    private final SpillSerializer<K> keySerializer;
    private final SpillSerializer<V> valueSerializer;
    private final ServerSocket server;
    private final Thread acceptor;
    private final List<Socket> clients = new ArrayList<>();
    private volatile boolean closed;

    // Server-side observability (Rub reaches the wire through these). All monotonic, thread-safe.
    private final AtomicLong connectionsAccepted = new AtomicLong();
    private final AtomicLong gets = new AtomicLong();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong sizeQueries = new AtomicLong();
    private final AtomicLong rangeQueries = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();

    /**
     * A point-in-time readout of the wire's own traffic: connections accepted and requests
     * served, broken out by op, plus refused requests. {@link #requestsServed()} is the sum of
     * the five op counters — the wire's contribution to the observability story.
     */
    public record WireStats(long connectionsAccepted, long gets, long puts, long deletes,
                            long sizeQueries, long rangeQueries, long errors) {
        /** Every well-formed request the wire answered — the five op counters summed. */
        public long requestsServed() {
            return gets + puts + deletes + sizeQueries + rangeQueries;
        }

        /** A one-line readout in the ecosystem's house shape. */
        public String line() {
            return String.format(
                    "conns=%d reqs=%d (get=%d put=%d del=%d size=%d range=%d) errors=%d",
                    connectionsAccepted, requestsServed(), gets, puts, deletes,
                    sizeQueries, rangeQueries, errors);
        }
    }

    private SmokeSignalServer(SmokeHouse<K, V> store, SpillSerializer<K> keySerializer,
                              SpillSerializer<V> valueSerializer, ServerSocket server) {
        this.store = store;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.server = server;
        this.acceptor = new Thread(this::acceptLoop, "smokesignal-acceptor");
        this.acceptor.setDaemon(true);
    }

    /** Put {@code store} on an ephemeral loopback port (see {@link #port()}). */
    public static <K, V> SmokeSignalServer<K, V> serve(SmokeHouse<K, V> store,
                                                       SpillSerializer<K> keySerializer,
                                                       SpillSerializer<V> valueSerializer)
            throws IOException {
        Objects.requireNonNull(store, "store");
        ServerSocket server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
        SmokeSignalServer<K, V> s = new SmokeSignalServer<>(store,
                Objects.requireNonNull(keySerializer, "keySerializer"),
                Objects.requireNonNull(valueSerializer, "valueSerializer"), server);
        s.acceptor.start();
        return s;
    }

    /** The loopback port clients connect to. */
    public int port() {
        return server.getLocalPort();
    }

    /** A snapshot of the wire's own traffic counters — the server side of observability. */
    public WireStats stats() {
        return new WireStats(connectionsAccepted.get(), gets.get(), puts.get(), deletes.get(),
                sizeQueries.get(), rangeQueries.get(), errors.get());
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket socket = server.accept();
                connectionsAccepted.incrementAndGet();
                synchronized (clients) {
                    clients.add(socket);
                }
                Thread t = new Thread(() -> session(socket), "smokesignal-session");
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                return;                                        // server closed
            }
        }
    }

    private void session(Socket socket) {
        try (socket) {
            DataInputStream in = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));
            while (true) {
                byte op;
                try {
                    op = in.readByte();
                } catch (EOFException clientDone) {
                    return;
                }
                try {
                    handle(op, in, out);
                } catch (RuntimeException e) {                 // bad request, store refusal…
                    errors.incrementAndGet();
                    out.writeByte(REPLY_ERROR);
                    out.writeUTF(String.valueOf(e.getMessage()));
                }
                out.flush();
            }
        } catch (IOException wireGone) {
            // client vanished; the store is untouched by a dead wire
        }
    }

    private void handle(byte op, DataInputStream in, DataOutputStream out) throws IOException {
        switch (op) {
            case OP_GET -> {
                gets.incrementAndGet();
                V value = store.get(keySerializer.read(in));
                if (value == null) {
                    out.writeByte(REPLY_NULL);
                } else {
                    out.writeByte(REPLY_VALUE);
                    valueSerializer.write(value, out);
                }
            }
            case OP_PUT -> {
                puts.incrementAndGet();
                K key = keySerializer.read(in);
                V value = valueSerializer.read(in);
                store.put(key, value);
                out.writeByte(REPLY_VALUE);
            }
            case OP_DELETE -> {
                deletes.incrementAndGet();
                boolean existed = store.delete(keySerializer.read(in));
                out.writeByte(REPLY_VALUE);
                out.writeBoolean(existed);
            }
            case OP_SIZE -> {
                sizeQueries.incrementAndGet();
                out.writeByte(REPLY_VALUE);
                out.writeInt(store.size());
            }
            case OP_COUNT_RANGE -> {
                rangeQueries.incrementAndGet();
                K lo = keySerializer.read(in);
                K hi = keySerializer.read(in);
                out.writeByte(REPLY_VALUE);
                out.writeInt(store.countRange(lo, hi));
            }
            default -> throw new IllegalArgumentException("unknown op " + op);
        }
    }

    /** Stop accepting and drop every session. The store itself is not closed. */
    @Override
    public void close() {
        closed = true;
        try {
            server.close();
        } catch (IOException ignored) {
            // teardown
        }
        synchronized (clients) {
            for (Socket s : clients) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // teardown
                }
            }
            clients.clear();
        }
    }
}
