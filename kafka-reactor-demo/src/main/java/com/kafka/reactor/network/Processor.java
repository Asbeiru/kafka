package com.kafka.reactor.network;

import com.kafka.reactor.common.MemoryPool;
import com.kafka.reactor.quota.ConnectionQuotas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processor (Sub Reactor) thread.
 * One Processor = One Thread + One Selector
 * Implements the 7-step event loop for handling I/O events.
 *
 * Aligns with Kafka's Processor class (SocketServer.scala:815-1283).
 */
public class Processor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Processor.class);

    // CRITICAL: Match Kafka's connection queue size
    private static final int CONNECTION_QUEUE_SIZE = 20;
    private static final long POLL_TIMEOUT_MS = 300;

    private final int id;
    private final String listenerName;
    private final RequestChannel requestChannel;
    private final ConnectionQuotas connectionQuotas;
    private final MemoryPool memoryPool;
    private final KafkaSelector selector;
    private final int maxReceiveSize;

    // CRITICAL FIX #3: Use ArrayBlockingQueue with capacity 20
    private final BlockingQueue<SocketChannel> newConnections = new ArrayBlockingQueue<>(CONNECTION_QUEUE_SIZE);
    private final AtomicBoolean running = new AtomicBoolean(true);

    private int connectionIndex = 0;

    public Processor(int id,
                     String listenerName,
                     RequestChannel requestChannel,
                     ConnectionQuotas connectionQuotas,
                     MemoryPool memoryPool,
                     int maxReceiveSize) throws IOException {
        this.id = id;
        this.listenerName = listenerName;
        this.requestChannel = requestChannel;
        this.connectionQuotas = connectionQuotas;
        this.memoryPool = memoryPool;
        this.maxReceiveSize = maxReceiveSize;
        this.selector = new KafkaSelector(memoryPool, maxReceiveSize);

        // Register this Processor's response queue
        requestChannel.addResponseQueue(id);
    }

    /**
     * CRITICAL FIX #2: Accept new connection with mayBlock parameter.
     * Implements backpressure mechanism.
     *
     * @param socketChannel The new connection
     * @param mayBlock Whether to block if queue is full
     * @return true if accepted, false if rejected
     */
    public boolean accept(SocketChannel socketChannel, boolean mayBlock) throws InterruptedException {
        if (newConnections.offer(socketChannel)) {
            // Queue has space, accepted immediately
            wakeup();
            return true;
        } else if (mayBlock) {
            // Queue is full, but this is the last attempt - block until space available
            log.debug("Processor {} newConnections queue full, blocking...", id);
            newConnections.put(socketChannel);
            wakeup();
            return true;
        } else {
            // Queue is full and we shouldn't block - try next Processor
            return false;
        }
    }

    /**
     * Wake up the selector from blocking select().
     * CRITICAL FIX #5: Use selector.wakeup() method.
     */
    private void wakeup() {
        selector.wakeup();
    }

    @Override
    public void run() {
        log.info("Processor {} started", id);

        try {
            while (running.get()) {
                try {
                    // ========== 7-STEP EVENT LOOP ==========

                    // Step 1: Configure new connections
                    configureNewConnections();

                    // Step 2: Process new responses
                    processNewResponses();

                    // Step 3: I/O multiplexing - select ready events
                    selector.poll(POLL_TIMEOUT_MS);

                    // Step 4: Process completed receives
                    processCompletedReceives();

                    // Step 5: Process completed sends
                    processCompletedSends();

                    // Step 6: Process disconnected connections
                    processDisconnected();

                    // Step 7: Close excess connections (simplified - not implemented)
                    // closeExcessConnections();

                } catch (Exception e) {
                    log.error("Error in processor {} event loop", id, e);
                }
            }
        } finally {
            shutdown();
        }
    }

    /**
     * Step 1: Configure new connections from the newConnections queue.
     * CRITICAL FIX #4: Process at most CONNECTION_QUEUE_SIZE connections per iteration.
     */
    private void configureNewConnections() {
        int connectionsProcessed = 0;

        while (connectionsProcessed < CONNECTION_QUEUE_SIZE && !newConnections.isEmpty()) {
            SocketChannel socketChannel = newConnections.poll();
            if (socketChannel != null) {
                try {
                    // Generate connection ID
                    String connectionId = generateConnectionId(socketChannel);

                    // Check connection quota
                    InetAddress address = socketChannel.socket().getInetAddress();
                    if (!connectionQuotas.inc(listenerName, address)) {
                        log.warn("Connection quota exceeded for {}, closing connection", address);
                        socketChannel.close();
                        connectionsProcessed++;
                        continue;
                    }

                    // Register with selector
                    selector.register(connectionId, socketChannel);
                    log.info("Processor {} accepted connection: {}", id, connectionId);

                } catch (IOException e) {
                    log.error("Error configuring new connection", e);
                    try {
                        socketChannel.close();
                    } catch (IOException ex) {
                        log.error("Error closing failed connection", ex);
                    }
                }
                connectionsProcessed++;
            }
        }
    }

    /**
     * Step 2: Process responses from Handler threads.
     */
    private void processNewResponses() {
        RequestChannel.Response response;
        while ((response = requestChannel.receiveResponse(id)) != null) {
            try {
                String connectionId = response.request.connectionId;

                // Create NetworkSend and queue for sending
                NetworkSend send = NetworkSend.createWithSize(connectionId, response.buffer);
                selector.send(send);

                // Unmute the connection to allow new requests
                selector.unmute(connectionId);

                log.debug("Processor {} queued response for {}", id, connectionId);

            } catch (Exception e) {
                log.error("Error processing response in processor {}", id, e);
            }
        }
    }

    /**
     * Step 4: Process completed receives - send to RequestChannel.
     */
    private void processCompletedReceives() {
        for (NetworkReceive receive : selector.completedReceives()) {
            try {
                String connectionId = receive.source();

                // Mute this connection until response is sent (guarantee ordering)
                selector.mute(connectionId);

                // Create request and send to RequestChannel
                RequestChannel.Request request = new RequestChannel.Request(
                        id,
                        connectionId,
                        receive.payload()
                );

                requestChannel.sendRequest(request);
                log.debug("Processor {} sent request from {} to RequestChannel", id, connectionId);

            } catch (Exception e) {
                log.error("Error processing receive in processor {}", id, e);
            }
        }
    }

    /**
     * Step 5: Process completed sends.
     */
    private void processCompletedSends() {
        for (NetworkSend send : selector.completedSends()) {
            log.debug("Processor {} completed send to {}", id, send.destination());
        }
    }

    /**
     * Step 6: Process disconnected connections.
     */
    private void processDisconnected() {
        for (String connectionId : selector.disconnected()) {
            try {
                // Decrement connection quota
                // Note: We'd need to track the InetAddress for each connection
                log.info("Processor {} connection disconnected: {}", id, connectionId);

            } catch (Exception e) {
                log.error("Error processing disconnect in processor {}", id, e);
            }
        }
    }

    /**
     * Generate connection ID: remoteHost:remotePort-localPort-index
     * Matches Kafka's format.
     */
    private String generateConnectionId(SocketChannel channel) {
        String remoteHost = channel.socket().getInetAddress().getHostAddress();
        int remotePort = channel.socket().getPort();
        int localPort = channel.socket().getLocalPort();
        int index = connectionIndex++;

        return String.format("%s:%d-%d-%d", remoteHost, remotePort, localPort, index);
    }

    public void shutdown() {
        running.set(false);
        try {
            selector.close();
            requestChannel.removeResponseQueue(id);
            log.info("Processor {} shutdown complete", id);
        } catch (IOException e) {
            log.error("Error shutting down processor {}", id, e);
        }
    }

    public int id() {
        return id;
    }

    @Override
    public String toString() {
        return "Processor{id=" + id + ", listenerName=" + listenerName + "}";
    }
}
