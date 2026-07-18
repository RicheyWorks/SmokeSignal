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
