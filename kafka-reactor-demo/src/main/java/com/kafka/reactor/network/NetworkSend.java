package com.kafka.reactor.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * Represents a network send operation.
 * Kafka message format: [4-byte size] + [payload]
 * Uses Scatter/Gather I/O for efficient writing.
 * Aligns with Kafka's NetworkSend class.
 */
public class NetworkSend {
    private static final Logger log = LoggerFactory.getLogger(NetworkSend.class);

    private final String destination;
    private final ByteBuffer[] buffers;
    private long totalSize;
    private long remaining;
    private boolean pending = true;

    public NetworkSend(String destination, ByteBuffer... buffers) {
        this.destination = destination;
        this.buffers = buffers;

        for (ByteBuffer buffer : buffers) {
            remaining += buffer.remaining();
        }
        totalSize = remaining;
    }

    /**
     * Create a NetworkSend with size header.
     * Format: [4-byte size] + [payload]
     */
    public static NetworkSend createWithSize(String destination, ByteBuffer payload) {
        int size = payload.remaining();
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(size);
        sizeBuffer.flip();
        return new NetworkSend(destination, sizeBuffer, payload);
    }

    /**
     * Write data to channel using Scatter/Gather I/O.
     */
    public long writeTo(GatheringByteChannel channel) throws IOException {
        long written = channel.write(buffers);
        remaining -= written;

        if (remaining <= 0) {
            pending = false;
            log.debug("Completed send of {} bytes to {}", totalSize, destination);
        }

        return written;
    }

    public boolean completed() {
        return !pending;
    }

    public String destination() {
        return destination;
    }

    public long size() {
        return totalSize;
    }

    public long remaining() {
        return remaining;
    }
}
