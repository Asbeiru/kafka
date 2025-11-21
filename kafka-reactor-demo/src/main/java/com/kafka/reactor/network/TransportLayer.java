package com.kafka.reactor.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Transport layer wrapping a SocketChannel.
 * Provides abstraction for network I/O operations.
 * Aligns with Kafka's TransportLayer interface and PlaintextTransportLayer implementation.
 */
public class TransportLayer {
    private static final Logger log = LoggerFactory.getLogger(TransportLayer.class);

    private final String channelId;
    private final SocketChannel socketChannel;
    private SelectionKey key;

    public TransportLayer(String channelId, SocketChannel socketChannel) throws IOException {
        this.channelId = channelId;
        this.socketChannel = socketChannel;
        this.socketChannel.configureBlocking(false);
    }

    public void setKey(SelectionKey key) {
        this.key = key;
    }

    public SelectionKey selectionKey() {
        return key;
    }

    public SocketChannel socketChannel() {
        return socketChannel;
    }

    public String channelId() {
        return channelId;
    }

    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    public void close() throws IOException {
        socketChannel.close();
        log.debug("Closed transport layer for {}", channelId);
    }

    public boolean finishConnect() throws IOException {
        boolean connected = socketChannel.finishConnect();
        if (connected) {
            log.debug("Connection established for {}", channelId);
        }
        return connected;
    }

    public boolean ready() {
        return socketChannel.isConnected();
    }

    public boolean hasPendingWrites() {
        return false; // Simplified implementation
    }
}
