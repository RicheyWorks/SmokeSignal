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
 *
 * <h2>The write route (2026-08-19)</h2>
 *
 * <p>Writes over the wire used to go straight to the served store — correct for a standalone
 * SmokeHouse, wrong the moment the store sits behind an {@code IndexedStore}, whose secondary
 * and interval indexes must see every mutation. That is the same lesson Twine's sink seam
 * taught, pointed at the wire: WholeHog's findings ledger carried it as "deliberately unsolved
 * until a consumer needs it", and the consumer arrived. {@link WriteRoute} is the seam:
 * {@link #serve(SmokeHouse, SpillSerializer, SpillSerializer)} keeps the old behavior (the
 * route IS the store), and {@link #serve(SmokeHouse, WriteRoute, SpillSerializer, SpillSerializer)}
 * lets a composed caller route wire writes through its index fan-out while reads stay on the
 * primary's read surface.</p>
 */
public final class SmokeSignalServer<K, V> implements Closeable {

    /**
     * Where the wire's writes land. The contract is the ecosystem's write-seam contract:
     * {@code put} is a last-writer-wins upsert, {@code delete} of an absent key is a no-op
     * that answers {@code false}. Reads are NOT routed — they stay on the served store,
     * because a route that answered reads differently from the store it fronts would make
     * the wire lie.
     */
    public interface WriteRoute<K, V> {
        /** Last-writer-wins upsert. */
        void put(K key, V value) throws IOException;

        /** Delete; answers whether the key existed (a no-op {@code false} when absent). */
        boolean delete(K key) throws IOException;
    }

    /** One op inside a wire batch: a put ({@code value != null}) or a delete. */
    public record BatchOp<K, V>(K key, V value) {
        public boolean isPut() {
            return value != null;
        }
    }

    /**
     * Where a wire BATCH lands, whole (2026-08-19): the server reads every op off the socket
     * first, then hands the complete list here in one call — so the route decides the batch's
     * atomicity, and the wire never applies half a request. The default route (see
     * {@link #serve(SmokeHouse, WriteRoute, SpillSerializer, SpillSerializer)}) applies ops
     * in order through the {@link WriteRoute}, which is sequential, not atomic; a caller with
     * a real atomic batcher (Twine is the ecosystem's) supplies its own and gets crash-atomic
     * batches from any wire client. Sessions run on their own threads — a route over a
     * one-at-a-time batcher must synchronize.
     */
    @FunctionalInterface
    public interface BatchRoute<K, V> {
        void apply(List<BatchOp<K, V>> ops) throws IOException;
    }

    static final byte OP_GET = 1;
    static final byte OP_PUT = 2;
    static final byte OP_DELETE = 3;
    static final byte OP_SIZE = 4;
    static final byte OP_COUNT_RANGE = 5;
    static final byte OP_BATCH = 6;                            // 2026-08-19: batches over the wire
    static final byte OP_RANGE = 7;                            // 2026-08-20: fetch a range's records
    static final byte OP_STATS = 8;                            // 2026-08-20: the wire's own meter
    static final byte BATCH_OP_PUT = 1;
    static final byte BATCH_OP_DELETE = 2;
    static final byte REPLY_NULL = 0;
    static final byte REPLY_VALUE = 1;
    static final byte REPLY_ERROR = 2;

    private final SmokeHouse<K, V> store;
    private final WriteRoute<K, V> writeRoute;
    private final BatchRoute<K, V> batchRoute;
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
    private final AtomicLong batchRequests = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();

    /**
     * A point-in-time readout of the wire's own traffic: connections accepted and requests
     * served, broken out by op, plus refused requests. {@link #requestsServed()} is the sum of
     * the six op counters — the wire's contribution to the observability story. ({@code batches}
     * counts BATCH requests, not the ops inside them — the ops land through the batch route,
     * whose owner meters them however it meters anything else.)
     */
    public record WireStats(long connectionsAccepted, long gets, long puts, long deletes,
                            long sizeQueries, long rangeQueries, long batches, long errors) {
        /** Every well-formed request the wire answered — the six op counters summed. */
        public long requestsServed() {
            return gets + puts + deletes + sizeQueries + rangeQueries + batches;
        }

        /**
         * A one-line readout in the ecosystem's house shape ({@link java.util.Locale#ROOT},
         * so the line never changes shape with the default locale).
         */
        public String line() {
            return String.format(java.util.Locale.ROOT,
                    "conns=%d reqs=%d (get=%d put=%d del=%d size=%d range=%d batch=%d) errors=%d",
                    connectionsAccepted, requestsServed(), gets, puts, deletes,
                    sizeQueries, rangeQueries, batches, errors);
        }
    }

    private SmokeSignalServer(SmokeHouse<K, V> store, WriteRoute<K, V> writeRoute,
                              BatchRoute<K, V> batchRoute,
                              SpillSerializer<K> keySerializer,
                              SpillSerializer<V> valueSerializer, ServerSocket server) {
        this.store = store;
        this.writeRoute = writeRoute;
        this.batchRoute = batchRoute;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.server = server;
        this.acceptor = new Thread(this::acceptLoop, "smokesignal-acceptor");
        this.acceptor.setDaemon(true);
    }

    /**
     * Put {@code store} on an ephemeral loopback port (see {@link #port()}). Writes over the
     * wire land on the store itself — correct for a standalone SmokeHouse; a store behind an
     * index fan-out should use the {@link WriteRoute} overload instead.
     */
    public static <K, V> SmokeSignalServer<K, V> serve(SmokeHouse<K, V> store,
                                                       SpillSerializer<K> keySerializer,
                                                       SpillSerializer<V> valueSerializer)
            throws IOException {
        Objects.requireNonNull(store, "store");
        return serve(store, new WriteRoute<K, V>() {
            @Override public void put(K key, V value) throws IOException { store.put(key, value); }
            @Override public boolean delete(K key) throws IOException { return store.delete(key); }
        }, keySerializer, valueSerializer);
    }

    /**
     * Put {@code store} on an ephemeral loopback port with an explicit write route: reads
     * answer from {@code store}, writes land through {@code writeRoute}. This is how a
     * composed caller (an {@code IndexedStore} owner) keeps the routing rule — writes go
     * through the index fan-out, never the primary — while still speaking the wire.
     */
    public static <K, V> SmokeSignalServer<K, V> serve(SmokeHouse<K, V> store,
                                                       WriteRoute<K, V> writeRoute,
                                                       SpillSerializer<K> keySerializer,
                                                       SpillSerializer<V> valueSerializer)
            throws IOException {
        Objects.requireNonNull(writeRoute, "writeRoute");
        // Default batch route: in order, through the write route — sequential, NOT atomic.
        return serve(store, writeRoute, ops -> {
            for (BatchOp<K, V> op : ops) {
                if (op.isPut()) {
                    writeRoute.put(op.key(), op.value());
                } else {
                    writeRoute.delete(op.key());
                }
            }
        }, keySerializer, valueSerializer);
    }

    /**
     * Full routing control: reads answer from {@code store}, single writes land through
     * {@code writeRoute}, and wire BATCHes land whole through {@code batchRoute} — the seam
     * a composed caller ties to a real atomic batcher (Twine), making crash-atomic multi-key
     * batches available to any wire client.
     */
    public static <K, V> SmokeSignalServer<K, V> serve(SmokeHouse<K, V> store,
                                                       WriteRoute<K, V> writeRoute,
                                                       BatchRoute<K, V> batchRoute,
                                                       SpillSerializer<K> keySerializer,
                                                       SpillSerializer<V> valueSerializer)
            throws IOException {
        Objects.requireNonNull(store, "store");
        ServerSocket server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
        SmokeSignalServer<K, V> s = new SmokeSignalServer<>(store,
                Objects.requireNonNull(writeRoute, "writeRoute"),
                Objects.requireNonNull(batchRoute, "batchRoute"),
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
                sizeQueries.get(), rangeQueries.get(), batchRequests.get(), errors.get());
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
                } catch (RuntimeException framing) {
                    // ADR wire-framing (2026-08-20): a RuntimeException out of handle() is a
                    // READ-phase failure — an unknown op, an unknown batch-op byte, or a
                    // serializer refusing a field. The stream position is now unknown, so the
                    // session cannot safely continue. Reply once, then CLOSE. (Execution
                    // failures never reach here — they are handled in-place as recoverable
                    // REPLY_ERRORs by execute(...).)
                    errors.incrementAndGet();
                    writeError(out, "unrecoverable request framing error; closing session", framing);
                    out.flush();
                    return;
                }
                out.flush();
            }
        } catch (IOException wireGone) {
            // client vanished; the store is untouched by a dead wire
        }
    }

    /** A reply-writing / store-touching action whose {@link RuntimeException} is recoverable. */
    @FunctionalInterface
    private interface Execution {
        void run() throws IOException;
    }

    /**
     * The execute phase (ADR wire-framing 2026-08-20): the request has been fully read, so the
     * stream is aligned and the reply slot is ours. A {@link RuntimeException} here (a store
     * refusal, a route throwing) is a recoverable {@code REPLY_ERROR} — the session keeps
     * serving. {@link IOException} (a broken store, a dead socket) propagates and ends the
     * session, exactly as before.
     */
    private void execute(DataOutputStream out, Execution body) throws IOException {
        try {
            body.run();
        } catch (RuntimeException recoverable) {
            errors.incrementAndGet();
            writeError(out, "request refused", recoverable);
        }
    }

    /** Write a REPLY_ERROR with a truncated message (S2w: writeUTF caps at 64KB). */
    private static void writeError(DataOutputStream out, String context, Throwable cause)
            throws IOException {
        out.writeByte(REPLY_ERROR);
        String msg = context + ": " + cause.getMessage();
        out.writeUTF(msg.length() > 40_000 ? msg.substring(0, 40_000) + "…(truncated)" : msg);
    }

    /**
     * Handle one request. Each case READS all its fields first — a throw during the read phase
     * (an unknown op, an unknown batch-op byte, a serializer refusing a field) propagates to the
     * session loop, which closes the session because the stream position is now unknown. The
     * store/route touch and the reply write happen inside {@link #execute}, where a
     * {@code RuntimeException} is a recoverable {@code REPLY_ERROR}.
     */
    private void handle(byte op, DataInputStream in, DataOutputStream out) throws IOException {
        switch (op) {
            case OP_GET -> {
                K key = keySerializer.read(in);                // read phase
                gets.incrementAndGet();
                execute(out, () -> {                           // execute phase
                    V value = store.get(key);
                    if (value == null) {
                        out.writeByte(REPLY_NULL);
                    } else {
                        out.writeByte(REPLY_VALUE);
                        valueSerializer.write(value, out);
                    }
                });
            }
            case OP_PUT -> {
                K key = keySerializer.read(in);
                V value = valueSerializer.read(in);
                puts.incrementAndGet();
                execute(out, () -> {
                    writeRoute.put(key, value);                // writes go through the route
                    out.writeByte(REPLY_VALUE);
                });
            }
            case OP_DELETE -> {
                K key = keySerializer.read(in);
                deletes.incrementAndGet();
                execute(out, () -> {
                    boolean existed = writeRoute.delete(key);
                    out.writeByte(REPLY_VALUE);
                    out.writeBoolean(existed);
                });
            }
            case OP_SIZE -> {
                sizeQueries.incrementAndGet();
                execute(out, () -> {
                    int size = store.size();               // value BEFORE the reply byte (S1w discipline)
                    out.writeByte(REPLY_VALUE);
                    out.writeInt(size);
                });
            }
            case OP_COUNT_RANGE -> {
                K lo = keySerializer.read(in);
                K hi = keySerializer.read(in);
                rangeQueries.incrementAndGet();
                execute(out, () -> {
                    // Twelfth pass: compute the count BEFORE writing any reply byte, exactly as
                    // OP_RANGE materializes before it writes. execute() turns a thrown
                    // RuntimeException into a REPLY_ERROR, but it cannot un-write bytes already sent
                    // — so a value-producing call that can throw (countRange runs the store's
                    // comparator, which a caller may supply and which may reject incomparable keys)
                    // must run before REPLY_VALUE, or the error reply lands AFTER it and desyncs the
                    // session. (Reachable only with a throwing comparator today, but the S1w framing
                    // ADR made "materialize before write" the rule; this was the one read op still
                    // breaking it.)
                    int count = store.countRange(lo, hi);
                    out.writeByte(REPLY_VALUE);
                    out.writeInt(count);
                });
            }
            case OP_RANGE -> {
                // Fetch [lo, hi]'s records, in key order (2026-08-20) — the read countRange
                // could always count but never deliver. Materialized server-side before the
                // reply (the store's range callback cannot straddle socket writes), so the
                // memory bound is the range's size: ask for sane ranges, the same honesty
                // countRange always demanded of its callers.
                K lo = keySerializer.read(in);
                K hi = keySerializer.read(in);
                rangeQueries.incrementAndGet();
                execute(out, () -> {
                    List<Object[]> records = new ArrayList<>();
                    store.range(lo, hi, (k, v) -> records.add(new Object[]{k, v}));
                    out.writeByte(REPLY_VALUE);
                    out.writeInt(records.size());
                    for (Object[] rec : records) {
                        @SuppressWarnings("unchecked") K k = (K) rec[0];
                        @SuppressWarnings("unchecked") V v = (V) rec[1];
                        keySerializer.write(k, out);
                        valueSerializer.write(v, out);
                    }
                });
            }
            case OP_STATS -> {
                // The wire's own meter, readable by its clients (2026-08-20). Deliberately
                // NOT metered itself: a meter that counts being read muddies every reading.
                execute(out, () -> {
                    WireStats s = stats();
                    out.writeByte(REPLY_VALUE);
                    out.writeLong(s.connectionsAccepted());
                    out.writeLong(s.gets());
                    out.writeLong(s.puts());
                    out.writeLong(s.deletes());
                    out.writeLong(s.sizeQueries());
                    out.writeLong(s.rangeQueries());
                    out.writeLong(s.batches());
                    out.writeLong(s.errors());
                });
            }
            case OP_BATCH -> {
                // Read the WHOLE batch off the socket first: the wire never applies half a
                // request, and a throw here (a bad op-type byte, a serializer refusal) is a
                // framing error that closes the session — the stream position is unknown.
                int count = in.readInt();
                if (count < 0) {
                    throw new IllegalArgumentException("negative batch size " + count);
                }
                // S4w: never pre-size to an untrusted count — grow naturally; a real batch is small.
                List<BatchOp<K, V>> ops = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    byte type = in.readByte();
                    K key = keySerializer.read(in);
                    if (type == BATCH_OP_PUT) {
                        ops.add(new BatchOp<>(key, valueSerializer.read(in)));
                    } else if (type == BATCH_OP_DELETE) {
                        ops.add(new BatchOp<>(key, null));
                    } else {
                        throw new IllegalArgumentException("unknown batch op type " + type);
                    }
                }
                batchRequests.incrementAndGet();
                execute(out, () -> {
                    batchRoute.apply(ops);
                    out.writeByte(REPLY_VALUE);
                    out.writeInt(ops.size());
                });
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
