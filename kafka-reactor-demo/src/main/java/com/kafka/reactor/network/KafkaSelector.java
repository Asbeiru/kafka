package com.kafka.reactor.network;

import com.kafka.reactor.common.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;

/**
 * NIO Selector wrapper for managing multiple connections.
 * Performs I/O multiplexing using Java NIO Selector.
 * Aligns with Kafka's Selector class.
 */
public class KafkaSelector {
    private static final Logger log = LoggerFactory.getLogger(KafkaSelector.class);

    private final Selector nioSelector;
    private final Map<String, KafkaChannel> channels;
    private final List<NetworkReceive> completedReceives;
    private final List<NetworkSend> completedSends;
    private final Set<String> disconnected;
    private final MemoryPool memoryPool;
    private final int maxReceiveSize;

    public KafkaSelector(MemoryPool memoryPool, int maxReceiveSize) throws IOException {
        this.nioSelector = Selector.open();
        this.channels = new HashMap<>();
        this.completedReceives = new ArrayList<>();
        this.completedSends = new ArrayList<>();
        this.disconnected = new HashSet<>();
        this.memoryPool = memoryPool;
        this.maxReceiveSize = maxReceiveSize;
    }

    /**
     * Register a new connection.
     */
    public void register(String id, SocketChannel socketChannel) throws IOException {
        TransportLayer transportLayer = new TransportLayer(id, socketChannel);
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_READ);

        KafkaChannel channel = new KafkaChannel(id, transportLayer, memoryPool, maxReceiveSize);
        transportLayer.setKey(key);
        key.attach(channel);

        channels.put(id, channel);
        log.info("Registered channel: {}", id);
    }

    /**
     * Perform I/O multiplexing - select ready channels.
     */
    public void poll(long timeoutMs) throws IOException {
        // Clear previous results
        completedReceives.clear();
        completedSends.clear();
        disconnected.clear();

        int readyKeys = nioSelector.select(timeoutMs);

        if (readyKeys > 0) {
            Set<SelectionKey> selectedKeys = nioSelector.selectedKeys();
            Iterator<SelectionKey> iterator = selectedKeys.iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();

                KafkaChannel channel = (KafkaChannel) key.attachment();

                try {
                    // Handle readable event
                    if (key.isReadable()) {
                        read(channel);
                    }

                    // Handle writable event
                    if (key.isWritable()) {
                        write(channel);
                    }
                } catch (IOException e) {
                    log.error("I/O error on channel {}", channel.id(), e);
                    close(channel);
                    disconnected.add(channel.id());
                }
            }
        }
    }

    /**
     * Read from a channel.
     */
    private void read(KafkaChannel channel) throws IOException {
        long bytesRead = channel.read();

        if (bytesRead < 0) {
            // Connection closed by remote
            log.info("Connection closed by remote: {}", channel.id());
            close(channel);
            disconnected.add(channel.id());
        } else if (channel.currentReceive() != null && channel.currentReceive().complete()) {
            // Receive completed
            NetworkReceive receive = channel.currentReceive();
            completedReceives.add(receive);
            channel.clearReceive();

            log.debug("Completed receive from {}: {} bytes", channel.id(), receive.size());
        }
    }

    /**
     * Write to a channel.
     * Aligns with Kafka's Selector.write() (Selector.java:445-458)
     */
    private void write(KafkaChannel channel) throws IOException {
        String nodeId = channel.id();
        long bytesSent = channel.write();

        // Get completed send if any
        NetworkSend send = channel.maybeCompleteSend();

        // We may complete the send with bytesSent < 1 if `TransportLayer.hasPendingWrites` was true
        if (send != null) {
            completedSends.add(send);
            log.debug("Completed send to {}: {} bytes", nodeId, send.size());
        }
    }

    /**
     * Send data to a channel.
     */
    public void send(NetworkSend send) {
        String connectionId = send.destination();
        KafkaChannel channel = channels.get(connectionId);

        if (channel == null) {
            log.warn("Attempt to send to non-existent connection: {}", connectionId);
            return;
        }

        try {
            channel.setSend(send);
            log.debug("Queued send to {}: {} bytes", connectionId, send.size());
        } catch (Exception e) {
            log.error("Error queuing send to {}", connectionId, e);
            close(channel);
            disconnected.add(connectionId);
        }
    }

    /**
     * Mute a channel - stop reading from it.
     */
    public void mute(String id) {
        KafkaChannel channel = channels.get(id);
        if (channel != null) {
            channel.mute();
        }
    }

    /**
     * Unmute a channel - resume reading from it.
     */
    public void unmute(String id) {
        KafkaChannel channel = channels.get(id);
        if (channel != null) {
            channel.unmute();
        }
    }

    /**
     * Close a channel.
     */
    public void close(String id) {
        KafkaChannel channel = channels.remove(id);
        if (channel != null) {
            close(channel);
        }
    }

    private void close(KafkaChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            log.error("Error closing channel {}", channel.id(), e);
        }
    }

    /**
     * Wake up the selector from blocking select().
     * CRITICAL: This method is required for the Processor.wakeup() mechanism.
     */
    public void wakeup() {
        nioSelector.wakeup();
    }

    public List<NetworkReceive> completedReceives() {
        return completedReceives;
    }

    public List<NetworkSend> completedSends() {
        return completedSends;
    }

    public Set<String> disconnected() {
        return disconnected;
    }

    public Map<String, KafkaChannel> channels() {
        return channels;
    }

    public void close() throws IOException {
        for (KafkaChannel channel : channels.values()) {
            close(channel);
        }
        channels.clear();
        nioSelector.close();
        log.info("Closed selector");
    }
}
