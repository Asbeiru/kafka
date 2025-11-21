package com.kafka.reactor.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Acceptor (Main Reactor) thread.
 * Accepts new connections and assigns them to Processors in Round-Robin fashion.
 * One Acceptor has exactly ONE ServerSocketChannel for listening.
 *
 * Aligns with Kafka's Acceptor class (SocketServer.scala:476-785).
 */
public class Acceptor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Acceptor.class);

    private static final long SELECT_TIMEOUT_MS = 500;

    private final String listenerName;
    private final InetSocketAddress address;
    private final List<Processor> processors;

    private ServerSocketChannel serverSocketChannel;
    private Selector nioSelector;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private int currentProcessorIndex = 0;

    public Acceptor(String listenerName, InetSocketAddress address, List<Processor> processors) {
        this.listenerName = listenerName;
        this.address = address;
        this.processors = processors;
    }

    public void init() throws IOException {
        // Create and configure ServerSocketChannel
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(address);

        // Create Selector
        nioSelector = Selector.open();

        // Register ServerSocketChannel with OP_ACCEPT
        serverSocketChannel.register(nioSelector, SelectionKey.OP_ACCEPT);

        log.info("Acceptor initialized on {}", address);
    }

    @Override
    public void run() {
        log.info("Acceptor started for listener: {}", listenerName);

        try {
            while (running.get()) {
                try {
                    // Select ready events
                    int ready = nioSelector.select(SELECT_TIMEOUT_MS);

                    if (ready > 0) {
                        Iterator<SelectionKey> iterator = nioSelector.selectedKeys().iterator();

                        while (iterator.hasNext()) {
                            SelectionKey key = iterator.next();
                            iterator.remove();

                            if (key.isAcceptable()) {
                                acceptConnection(key);
                            }
                        }
                    }
                } catch (IOException e) {
                    log.error("Error in acceptor event loop", e);
                }
            }
        } finally {
            shutdown();
        }
    }

    /**
     * Accept a new connection and assign to a Processor.
     * Implements Round-Robin with backpressure (mayBlock logic).
     */
    private void acceptConnection(SelectionKey key) {
        try {
            // Get the ServerSocketChannel
            ServerSocketChannel server = (ServerSocketChannel) key.channel();

            // Accept the client SocketChannel
            SocketChannel socketChannel = server.accept();

            if (socketChannel != null) {
                socketChannel.configureBlocking(false);
                socketChannel.socket().setTcpNoDelay(true);
                socketChannel.socket().setKeepAlive(true);

                log.debug("Accepted connection from {}", socketChannel.socket().getRemoteSocketAddress());

                // CRITICAL FIX #1: Assign to Processor with mayBlock retry logic
                assignToProcessor(socketChannel);
            }
        } catch (IOException e) {
            log.error("Error accepting connection", e);
        }
    }

    /**
     * CRITICAL FIX #1: Assign connection to Processor with mayBlock retry logic.
     * Implements Kafka's Round-Robin with backpressure strategy:
     * - Try N-1 Processors with non-blocking offer()
     * - Last attempt uses blocking put() with mayBlock=true
     *
     * Source: SocketServer.scala:652-667
     */
    private void assignToProcessor(SocketChannel socketChannel) throws IOException {
        int retriesLeft = processors.size();
        boolean accepted = false;

        while (retriesLeft > 0 && !accepted) {
            retriesLeft--;

            // Select next Processor (Round-Robin)
            Processor processor = processors.get(currentProcessorIndex);
            currentProcessorIndex = (currentProcessorIndex + 1) % processors.size();

            // Last attempt should block, others should not
            boolean mayBlock = (retriesLeft == 0);

            try {
                accepted = processor.accept(socketChannel, mayBlock);

                if (accepted) {
                    log.debug("Assigned connection to processor {}", processor.id());
                } else {
                    log.debug("Processor {} queue full, trying next processor (retriesLeft={})",
                            processor.id(), retriesLeft);
                }
            } catch (InterruptedException e) {
                log.error("Interrupted while assigning connection to processor", e);
                Thread.currentThread().interrupt();
                socketChannel.close();
                return;
            }
        }

        if (!accepted) {
            // Should not happen if last attempt uses blocking put()
            log.error("Failed to assign connection after trying all processors");
            socketChannel.close();
        }
    }

    public void shutdown() {
        running.set(false);
        try {
            if (nioSelector != null) {
                nioSelector.close();
            }
            if (serverSocketChannel != null) {
                serverSocketChannel.close();
            }
            log.info("Acceptor shutdown complete");
        } catch (IOException e) {
            log.error("Error shutting down acceptor", e);
        }
    }

    @Override
    public String toString() {
        return "Acceptor{listenerName=" + listenerName + ", address=" + address + "}";
    }
}
