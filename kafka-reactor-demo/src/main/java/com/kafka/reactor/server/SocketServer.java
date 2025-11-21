package com.kafka.reactor.server;

import com.kafka.reactor.common.MemoryPool;
import com.kafka.reactor.common.SimpleMemoryPool;
import com.kafka.reactor.network.Acceptor;
import com.kafka.reactor.network.Processor;
import com.kafka.reactor.network.RequestChannel;
import com.kafka.reactor.quota.ConnectionQuotas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Main server class implementing Kafka's Multi-Reactor network model.
 *
 * Architecture:
 * - 1 Acceptor (Main Reactor) - accepts new connections
 * - N Processors (Sub Reactors, default 3) - handle I/O events
 * - M Handlers (default 8) - process business logic
 * - 1 RequestChannel - shared by all components
 *
 * Aligns with Kafka's SocketServer class.
 */
public class SocketServer {
    private static final Logger log = LoggerFactory.getLogger(SocketServer.class);

    // Default configuration
    private static final int DEFAULT_NUM_PROCESSORS = 3;
    private static final int DEFAULT_NUM_HANDLERS = 8;
    private static final int DEFAULT_REQUEST_QUEUE_SIZE = 500;
    private static final int DEFAULT_MAX_CONNECTIONS_PER_IP = 100;
    private static final int DEFAULT_MAX_CONNECTIONS = 10000;
    private static final long DEFAULT_MEMORY_POOL_SIZE = 100 * 1024 * 1024L; // 100MB
    private static final int DEFAULT_MAX_RECEIVE_SIZE = 1024 * 1024; // 1MB

    private final String listenerName;
    private final InetSocketAddress address;
    private final int numProcessors;
    private final int numHandlers;

    private RequestChannel requestChannel;
    private ConnectionQuotas connectionQuotas;
    private MemoryPool memoryPool;

    private Acceptor acceptor;
    private List<Processor> processors;
    private List<Thread> processorThreads;
    private List<RequestHandler> handlers;
    private List<Thread> handlerThreads;

    public SocketServer(String listenerName, InetSocketAddress address) {
        this(listenerName, address, DEFAULT_NUM_PROCESSORS, DEFAULT_NUM_HANDLERS);
    }

    public SocketServer(String listenerName, InetSocketAddress address, int numProcessors, int numHandlers) {
        this.listenerName = listenerName;
        this.address = address;
        this.numProcessors = numProcessors;
        this.numHandlers = numHandlers;
    }

    /**
     * Start the server.
     */
    public void start() throws IOException {
        log.info("Starting SocketServer on {}", address);

        // Initialize shared components
        requestChannel = new RequestChannel(DEFAULT_REQUEST_QUEUE_SIZE);
        connectionQuotas = new ConnectionQuotas(DEFAULT_MAX_CONNECTIONS_PER_IP, DEFAULT_MAX_CONNECTIONS);
        memoryPool = new SimpleMemoryPool(DEFAULT_MEMORY_POOL_SIZE, true);

        log.info("Initialized RequestChannel with queue size: {}", DEFAULT_REQUEST_QUEUE_SIZE);
        log.info("Initialized ConnectionQuotas - maxPerIp: {}, maxTotal: {}",
                DEFAULT_MAX_CONNECTIONS_PER_IP, DEFAULT_MAX_CONNECTIONS);
        log.info("Initialized MemoryPool with size: {} bytes", DEFAULT_MEMORY_POOL_SIZE);

        // Create and start Processors
        processors = new ArrayList<>();
        processorThreads = new ArrayList<>();

        for (int i = 0; i < numProcessors; i++) {
            Processor processor = new Processor(
                    i,
                    listenerName,
                    requestChannel,
                    connectionQuotas,
                    memoryPool,
                    DEFAULT_MAX_RECEIVE_SIZE
            );

            processors.add(processor);

            Thread thread = new Thread(processor, "processor-" + i);
            thread.setDaemon(false);
            thread.start();
            processorThreads.add(thread);

            log.info("Started Processor {}", i);
        }

        // Create and start Acceptor
        acceptor = new Acceptor(listenerName, address, processors);
        acceptor.init();

        Thread acceptorThread = new Thread(acceptor, "acceptor-" + listenerName);
        acceptorThread.setDaemon(false);
        acceptorThread.start();

        log.info("Started Acceptor for listener: {}", listenerName);

        // Create and start Request Handlers
        handlers = new ArrayList<>();
        handlerThreads = new ArrayList<>();

        for (int i = 0; i < numHandlers; i++) {
            RequestHandler handler = new RequestHandler(i, requestChannel);
            handlers.add(handler);

            Thread thread = new Thread(handler, "handler-" + i);
            thread.setDaemon(false);
            thread.start();
            handlerThreads.add(thread);

            log.info("Started RequestHandler {}", i);
        }

        log.info("=========================================");
        log.info("SocketServer started successfully!");
        log.info("Listener: {} on {}", listenerName, address);
        log.info("Architecture:");
        log.info("  - 1 Acceptor (Main Reactor)");
        log.info("  - {} Processors (Sub Reactors)", numProcessors);
        log.info("  - {} Handlers (Business Threads)", numHandlers);
        log.info("  - 1 RequestChannel (Shared Queue)");
        log.info("=========================================");
    }

    /**
     * Shutdown the server.
     */
    public void shutdown() {
        log.info("Shutting down SocketServer...");

        // Shutdown Acceptor
        if (acceptor != null) {
            acceptor.shutdown();
        }

        // Shutdown Processors
        if (processors != null) {
            for (Processor processor : processors) {
                processor.shutdown();
            }
        }

        // Shutdown Handlers
        if (handlers != null) {
            for (RequestHandler handler : handlers) {
                handler.shutdown();
            }
        }

        // Wait for threads to finish
        try {
            for (Thread thread : processorThreads) {
                thread.join(5000);
            }
            for (Thread thread : handlerThreads) {
                thread.join(5000);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for threads to finish", e);
        }

        log.info("SocketServer shutdown complete");
    }

    /**
     * Main method to run the server.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        // Parse arguments
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9092;

        InetSocketAddress address = new InetSocketAddress(host, port);
        SocketServer server = new SocketServer("PLAINTEXT", address);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered");
            server.shutdown();
        }));

        // Start server
        server.start();

        // Keep main thread alive
        Thread.currentThread().join();
    }
}
