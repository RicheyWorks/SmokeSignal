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

    /** The newest value for {@code key}, or {@code null} — a wire round-trip. */
    public synchronized V get(K key) throws IOException {
        out.writeByte(SmokeSignalServer.OP_GET);
        keySerializer.write(key, out);
        out.flush();
        byte reply = readReply();
        return reply == SmokeSignalServer.REPLY_NULL ? null : valueSerializer.read(in);
    }

    public synchronized void put(K key, V value) throws IOException {
        out.writeByte(SmokeSignalServer.OP_PUT);
        keySerializer.write(key, out);
        valueSerializer.write(value, out);
        out.flush();
        readReply();
    }

    public synchronized boolean delete(K key) throws IOException {
        out.writeByte(SmokeSignalServer.OP_DELETE);
        keySerializer.write(key, out);
        out.flush();
        readReply();
        return in.readBoolean();
    }

    public synchronized int size() throws IOException {
        out.writeByte(SmokeSignalServer.OP_SIZE);
        out.flush();
        readReply();
        return in.readInt();
    }

    public synchronized int countRange(K lo, K hi) throws IOException {
        out.writeByte(SmokeSignalServer.OP_COUNT_RANGE);
        keySerializer.write(lo, out);
        keySerializer.write(hi, out);
        out.flush();
        readReply();
        return in.readInt();
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
            committed = true;
            synchronized (SmokeSignalClient.this) {
                out.writeByte(SmokeSignalServer.OP_BATCH);
                out.writeInt(ops.size());
                for (java.util.Map.Entry<K, V> op : ops) {
                    if (op.getValue() != null) {
                        out.writeByte(SmokeSignalServer.BATCH_OP_PUT);
                        keySerializer.write(op.getKey(), out);
                        valueSerializer.write(op.getValue(), out);
                    } else {
                        out.writeByte(SmokeSignalServer.BATCH_OP_DELETE);
                        keySerializer.write(op.getKey(), out);
                    }
                }
                out.flush();
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
