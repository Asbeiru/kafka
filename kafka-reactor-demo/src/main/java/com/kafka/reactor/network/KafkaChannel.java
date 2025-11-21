package com.kafka.reactor.network;

import com.kafka.reactor.common.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * Represents a connection to a client.
 * Manages mute/unmute state and I/O operations.
 * Aligns with Kafka's KafkaChannel class.
 */
public class KafkaChannel {
    private static final Logger log = LoggerFactory.getLogger(KafkaChannel.class);

    private final String id;
    private final TransportLayer transportLayer;
    private final MemoryPool memoryPool;
    private final int maxReceiveSize;

    private NetworkReceive receive;
    private NetworkSend send;
    private boolean muted = false;

    public KafkaChannel(String id, TransportLayer transportLayer, MemoryPool memoryPool, int maxReceiveSize) {
        this.id = id;
        this.transportLayer = transportLayer;
        this.memoryPool = memoryPool;
        this.maxReceiveSize = maxReceiveSize;
    }

    public String id() {
        return id;
    }

    public TransportLayer transportLayer() {
        return transportLayer;
    }

    /**
     * Get the SelectionKey for this channel.
     * Required for mute/unmute operations.
     */
    public SelectionKey selectionKey() {
        return transportLayer.selectionKey();
    }

    /**
     * Mute this channel - stop reading data.
     * Removes OP_READ interest from the SelectionKey.
     */
    public void mute() {
        if (!muted) {
            SelectionKey key = selectionKey();
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
                muted = true;
                log.debug("Muted channel {}", id);
            }
        }
    }

    /**
     * Unmute this channel - resume reading data.
     * Adds OP_READ interest to the SelectionKey.
     */
    public void unmute() {
        if (muted) {
            SelectionKey key = selectionKey();
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
                muted = false;
                log.debug("Unmuted channel {}", id);
            }
        }
    }

    public boolean isMuted() {
        return muted;
    }

    /**
     * Read data from the network.
     */
    public long read() throws IOException {
        if (receive == null) {
            receive = new NetworkReceive(id, maxReceiveSize, memoryPool);
        }

        long bytesRead = receive.readFrom(transportLayer.socketChannel());

        if (receive.complete()) {
            log.debug("Completed receive from {}: {} bytes", id, receive.size());
        }

        return bytesRead;
    }

    /**
     * Write data to the network.
     */
    public long write() throws IOException {
        if (send == null) {
            return 0;
        }

        long bytesWritten = send.writeTo(transportLayer.socketChannel());

        if (send.completed()) {
            log.debug("Completed send to {}: {} bytes", id, send.size());
        }

        return bytesWritten;
    }

    public void setSend(NetworkSend send) {
        if (this.send != null && !this.send.completed()) {
            throw new IllegalStateException("Attempt to set a new send while previous send is not completed");
        }
        this.send = send;

        // Add OP_WRITE interest when there's data to send
        SelectionKey key = selectionKey();
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    public NetworkReceive currentReceive() {
        return receive;
    }

    /**
     * Returns the receive that has been completed, or null if the receive has not been completed.
     * The receive is only cleared from the channel once this method is invoked.
     *
     * IMPORTANT: Unlike clearReceive(), this method does NOT close the receive or release its memory.
     * The receive will be used by Processor to create a Request and passed to Handler.
     * Memory will be released later when the request is fully processed.
     *
     * Aligns with Kafka's KafkaChannel.maybeCompleteReceive() (KafkaChannel.java:258-265)
     */
    public NetworkReceive maybeCompleteReceive() {
        if (receive != null && receive.complete()) {
            NetworkReceive result = receive;
            receive = null;  // Clear from channel but DON'T close it
            return result;
        }
        return null;
    }

    public NetworkSend currentSend() {
        return send;
    }

    /**
     * Returns true if there is a send in progress.
     */
    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the send that has been completed, or null if the send has not been completed.
     * The send is only cleared from the channel once this method is invoked.
     *
     * Aligns with Kafka's KafkaChannel.maybeCompleteSend() (KafkaChannel.java:245-253)
     */
    public NetworkSend maybeCompleteSend() {
        if (send != null && send.completed()) {
            NetworkSend result = send;
            send = null;  // Clear the send

            // Remove OP_WRITE interest
            SelectionKey key = selectionKey();
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }

            return result;
        }
        return null;
    }

    public boolean hasBytesBuffered() {
        return transportLayer.hasPendingWrites();
    }

    public void clearReceive() {
        if (receive != null) {
            receive.close();
            receive = null;
        }
    }

    /**
     * Clear the send and remove OP_WRITE interest.
     * This method is used during channel close.
     */
    private void clearSend() {
        send = null;
        SelectionKey key = selectionKey();
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    public void close() throws IOException {
        clearReceive();
        clearSend();
        transportLayer.close();
        log.info("Closed channel {}", id);
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    @Override
    public String toString() {
        return "KafkaChannel{id=" + id + ", muted=" + muted + "}";
    }
}
