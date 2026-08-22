package io.github.richeyworks.smokesignal;

import io.github.richeyworks.superbeefsort.external.SpillSerializer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Objects;

/**
 * The matching client: one socket, synchronous request/reply, the same serializers the store
 * itself uses. Thread-safe by one lock per client (open several clients for parallelism —
 * they interleave at the store's single writer exactly like local threads would).
 */
public final class SmokeSignalClient<K, V> implements Closeable {

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    private SmokeSignalClient(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    private SpillSerializer<K> keySerializer;
    private SpillSerializer<V> valueSerializer;

    /** Connect to a {@link SmokeSignalServer} on the loopback {@code port}. */
    public static <K, V> SmokeSignalClient<K, V> connect(int port,
                                                         SpillSerializer<K> keySerializer,
                                                         SpillSerializer<V> valueSerializer)
            throws IOException {
        SmokeSignalClient<K, V> c = new SmokeSignalClient<>(
                new Socket(InetAddress.getLoopbackAddress(), port));
        c.keySerializer = Objects.requireNonNull(keySerializer, "keySerializer");
        c.valueSerializer = Objects.requireNonNull(valueSerializer, "valueSerializer");
        return c;
    }

    /** Builds a request into a private buffer — its throw sends nothing (S3w). */
    @FunctionalInterface
    private interface FrameBuilder {
        void build(DataOutputStream framed) throws IOException;
    }

    /**
     * Serialize a whole request into memory, then write it to the socket in one shot (tenth-pass
     * S3w). A serializer throwing mid-build sends NOTHING, so the client stays aligned and
     * usable — the old code wrote fields straight to the socket and a mid-request throw left a
     * partial frame buffered that the next call would flush, desyncing the server.
     */
    private void sendFrame(FrameBuilder builder) throws IOException {
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        DataOutputStream framed = new DataOutputStream(buf);
        builder.build(framed);                             // may throw — nothing on the wire yet
        framed.flush();
        out.write(buf.toByteArray());
        out.flush();
    }

    /** The newest value for {@code key}, or {@code null} — a wire round-trip. */
    public synchronized V get(K key) throws IOException {
        sendFrame(f -> {
            f.writeByte(SmokeSignalServer.OP_GET);
            keySerializer.write(key, f);
        });
        byte reply = readReply();
        return reply == SmokeSignalServer.REPLY_NULL ? null : valueSerializer.read(in);
    }

    public synchronized void put(K key, V value) throws IOException {
        sendFrame(f -> {
            f.writeByte(SmokeSignalServer.OP_PUT);
            keySerializer.write(key, f);
            valueSerializer.write(value, f);
        });
        readReply();
    }

    public synchronized boolean delete(K key) throws IOException {
        sendFrame(f -> {
            f.writeByte(SmokeSignalServer.OP_DELETE);
            keySerializer.write(key, f);
        });
        readReply();
        return in.readBoolean();
    }

    public synchronized int size() throws IOException {
        sendFrame(f -> f.writeByte(SmokeSignalServer.OP_SIZE));
        readReply();
        return in.readInt();
    }

    public synchronized int countRange(K lo, K hi) throws IOException {
        sendFrame(f -> {
            f.writeByte(SmokeSignalServer.OP_COUNT_RANGE);
            keySerializer.write(lo, f);
            keySerializer.write(hi, f);
        });
        readReply();
        return in.readInt();
    }

    /**
     * The 1-indexed rank of {@code key} among the live keys, or {@code 0} if absent — the store's
     * order-statistics surface over the wire (2026-08-21). O(log n) on the server, no scan.
     */
    public synchronized int rankOf(K key) throws IOException {
        sendFrame(f -> {
            f.writeByte(SmokeSignalServer.OP_RANK);
            keySerializer.write(key, f);
        });
        readReply();
        return in.readInt();
    }

    /**
     * The {@code rank}-th smallest live key, 1-indexed (1 = minimum, {@code size()} = maximum). A
     * rank outside {@code [1, size]} — including any rank on an empty store — surfaces the store's
     * refusal as an {@link IOException} (the wire's form of {@code store.nthKey}'s
     * {@code IndexOutOfBoundsException}); the session stays aligned and usable.
     */
    public synchronized K nthKey(int rank) throws IOException {
        sendFrame(f -> {
            f.writeByte(SmokeSignalServer.OP_NTH);
            f.writeInt(rank);
        });
        byte reply = readReply();
        return reply == SmokeSignalServer.REPLY_NULL ? null : keySerializer.read(in);
    }

    /** The lower-median live key, or {@code null} if the store is empty. */
    public synchronized K medianKey() throws IOException {
        sendFrame(f -> f.writeByte(SmokeSignalServer.OP_MEDIAN));
        byte reply = readReply();
        return reply == SmokeSignalServer.REPLY_NULL ? null : keySerializer.read(in);
    }

    /**
     * Fetch every record in {@code [lo, hi]} (both inclusive, by the store's comparator), in
     * key order (2026-08-20) — {@link #countRange} could always count them; this delivers
     * them. The reply is materialized, so the memory bound on both ends is the range's size:
     * ask for sane ranges, the same honesty countRange always demanded.
     */
    public synchronized java.util.LinkedHashMap<K, V> rangeQuery(K lo, K hi) throws IOException {
        sendFrame(f -> {
            f.writeByte(SmokeSignalServer.OP_RANGE);
            keySerializer.write(lo, f);
            keySerializer.write(hi, f);
        });
        readReply();
        int count = in.readInt();
        java.util.LinkedHashMap<K, V> records = new java.util.LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            records.put(keySerializer.read(in), valueSerializer.read(in));
        }
        return records;
    }

    /**
     * The server's own traffic meter, over the wire (2026-08-20) — observability reaching the
     * wire's clients. Reading it is deliberately not metered.
     */
    public synchronized SmokeSignalServer.WireStats stats() throws IOException {
        sendFrame(f -> f.writeByte(SmokeSignalServer.OP_STATS));
        readReply();
        return new SmokeSignalServer.WireStats(in.readLong(), in.readLong(), in.readLong(),
                in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readLong());
    }

    /**
     * Stage a batch of puts and deletes to send as ONE wire request (2026-08-19). The server
     * reads the whole batch before touching its route, and hands it to the route whole — so
     * when the served store's owner routed batches to a real atomic batcher (Twine, in the
     * ecosystem), a wire client gets crash-atomic multi-key batches with this call. Nothing
     * is sent until {@link Batch#commit()}.
     */
    public Batch batch() {
        return new Batch();
    }

    /** A staged wire batch. Ops keep their staged order on the server. */
    public final class Batch {

        private final java.util.List<java.util.Map.Entry<K, V>> ops = new java.util.ArrayList<>();
        private boolean committed;

        public Batch put(K key, V value) {
            requireStaging();
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            ops.add(java.util.Map.entry(key, value));
            return this;
        }

        public Batch delete(K key) {
            requireStaging();
            Objects.requireNonNull(key, "key");
            ops.add(new java.util.AbstractMap.SimpleImmutableEntry<>(key, null));
            return this;
        }

        /** Send the batch as one request; answers how many ops the server applied. */
        public int commit() throws IOException {
            requireStaging();
            synchronized (SmokeSignalClient.this) {
                sendFrame(f -> {                           // S3w: built in memory, sent whole
                    f.writeByte(SmokeSignalServer.OP_BATCH);
                    f.writeInt(ops.size());
                    for (java.util.Map.Entry<K, V> op : ops) {
                        if (op.getValue() != null) {
                            f.writeByte(SmokeSignalServer.BATCH_OP_PUT);
                            keySerializer.write(op.getKey(), f);
                            valueSerializer.write(op.getValue(), f);
                        } else {
                            f.writeByte(SmokeSignalServer.BATCH_OP_DELETE);
                            keySerializer.write(op.getKey(), f);
                        }
                    }
                });
                committed = true;                          // only after the whole frame is on the wire
                readReply();
                return in.readInt();
            }
        }

        private void requireStaging() {
            if (committed) {
                throw new IllegalStateException("batch already committed");
            }
        }
    }

    private byte readReply() throws IOException {
        byte reply = in.readByte();
        if (reply == SmokeSignalServer.REPLY_ERROR) {
            throw new IOException("server refused: " + in.readUTF());
        }
        return reply;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
