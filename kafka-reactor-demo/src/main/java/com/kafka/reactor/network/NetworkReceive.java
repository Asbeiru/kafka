package com.kafka.reactor.network;

import com.kafka.reactor.common.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Represents a network receive operation.
 * Kafka message format: [4-byte size] + [payload]
 * Aligns with Kafka's NetworkReceive class.
 */
public class NetworkReceive {
    private static final Logger log = LoggerFactory.getLogger(NetworkReceive.class);
    public static final int UNLIMITED = -1;

    private final String source;
    private final MemoryPool memoryPool;
    private final int maxSize;

    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    private ByteBuffer payloadBuffer;
    private int size = -1;
    private boolean complete = false;

    public NetworkReceive(String source, MemoryPool memoryPool) {
        this(source, UNLIMITED, memoryPool);
    }

    public NetworkReceive(String source, int maxSize, MemoryPool memoryPool) {
        this.source = source;
        this.maxSize = maxSize;
        this.memoryPool = memoryPool;
    }

    /**
     * Read data from channel.
     * Two-phase reading:
     * 1. Read 4-byte size header
     * 2. Read payload based on size
     */
    public long readFrom(ReadableByteChannel channel) throws IOException {
        long totalBytesRead = 0;

        // Phase 1: Read 4-byte size header
        if (size == -1) {
            int bytesRead = channel.read(sizeBuffer);
            if (bytesRead < 0) {
                throw new IOException("Connection closed while reading size");
            }
            totalBytesRead += bytesRead;

            if (!sizeBuffer.hasRemaining()) {
                sizeBuffer.flip();
                size = sizeBuffer.getInt();

                if (size < 0) {
                    throw new IOException("Invalid message size: " + size);
                }
                if (maxSize != UNLIMITED && size > maxSize) {
                    throw new IOException("Message size " + size + " exceeds max size " + maxSize);
                }

                log.debug("Read message size: {} bytes from {}", size, source);

                // Allocate buffer for payload
                payloadBuffer = memoryPool.tryAllocate(size);
                if (payloadBuffer == null) {
                    throw new IOException("Failed to allocate " + size + " bytes from memory pool");
                }
            }
        }

        // Phase 2: Read payload
        if (payloadBuffer != null && payloadBuffer.hasRemaining()) {
            int bytesRead = channel.read(payloadBuffer);
            if (bytesRead < 0) {
                throw new IOException("Connection closed while reading payload");
            }
            totalBytesRead += bytesRead;

            if (!payloadBuffer.hasRemaining()) {
                payloadBuffer.flip();
                complete = true;
                log.debug("Completed receive of {} bytes from {}", size, source);
            }
        }

        return totalBytesRead;
    }

    public boolean complete() {
        return complete;
    }

    public String source() {
        return source;
    }

    public ByteBuffer payload() {
        return payloadBuffer;
    }

    public int size() {
        return size;
    }

    public void close() {
        if (payloadBuffer != null) {
            memoryPool.release(payloadBuffer);
            payloadBuffer = null;
        }
    }
}
