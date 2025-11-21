package com.kafka.reactor.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple memory pool implementation using CAS (Compare-And-Swap) operations.
 * Aligns with Kafka's SimpleMemoryPool.
 */
public class SimpleMemoryPool implements MemoryPool {
    private static final Logger log = LoggerFactory.getLogger(SimpleMemoryPool.class);

    private final long sizeBytes;
    private final AtomicLong availableMemory;
    private final boolean strict;

    public SimpleMemoryPool(long sizeBytes, boolean strict) {
        this.sizeBytes = sizeBytes;
        this.availableMemory = new AtomicLong(sizeBytes);
        this.strict = strict;
    }

    @Override
    public ByteBuffer tryAllocate(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Size must be non-negative");
        }

        long current;
        long newValue;
        do {
            current = availableMemory.get();
            newValue = current - size;

            if (strict && newValue < 0) {
                log.warn("Memory pool out of memory. Requested: {}, Available: {}", size, current);
                return null;
            }
        } while (!availableMemory.compareAndSet(current, newValue));

        log.debug("Allocated {} bytes, available memory: {} -> {}", size, current, newValue);
        return ByteBuffer.allocate(size);
    }

    @Override
    public void release(ByteBuffer previouslyAllocated) {
        if (previouslyAllocated == null) {
            return;
        }

        int size = previouslyAllocated.capacity();
        long current = availableMemory.addAndGet(size);
        log.debug("Released {} bytes, available memory: {}", size, current);
    }

    @Override
    public long size() {
        return sizeBytes;
    }

    @Override
    public long availableMemory() {
        return Math.max(0, availableMemory.get());
    }

    @Override
    public boolean isOutOfMemory() {
        return availableMemory() == 0;
    }

    @Override
    public String toString() {
        return "SimpleMemoryPool{" +
                "sizeBytes=" + sizeBytes +
                ", availableMemory=" + availableMemory.get() +
                ", strict=" + strict +
                '}';
    }
}
