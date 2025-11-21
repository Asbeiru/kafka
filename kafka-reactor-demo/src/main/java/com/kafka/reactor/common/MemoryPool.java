package com.kafka.reactor.common;

import java.nio.ByteBuffer;

/**
 * Memory pool interface for managing ByteBuffer allocation.
 * Aligns with Kafka's MemoryPool interface.
 */
public interface MemoryPool {
    /**
     * Try to allocate a buffer of the specified size.
     * @param sizeBytes The size in bytes
     * @return A ByteBuffer or null if allocation fails
     */
    ByteBuffer tryAllocate(int sizeBytes);

    /**
     * Release a previously allocated buffer back to the pool.
     * @param previouslyAllocated The buffer to release
     */
    void release(ByteBuffer previouslyAllocated);

    /**
     * Get the total memory size of the pool.
     * @return Total size in bytes
     */
    long size();

    /**
     * Get the available memory in the pool.
     * @return Available size in bytes
     */
    long availableMemory();

    /**
     * Check if the pool is out of memory.
     * @return true if out of memory
     */
    boolean isOutOfMemory();
}
