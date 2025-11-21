package com.kafka.reactor.server;

import com.kafka.reactor.network.RequestChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Request handler (business thread) for processing requests.
 * Pulls requests from RequestChannel, processes them, and sends responses back.
 *
 * Aligns with Kafka's KafkaRequestHandler.
 */
public class RequestHandler implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RequestHandler.class);

    private final int id;
    private final RequestChannel requestChannel;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public RequestHandler(int id, RequestChannel requestChannel) {
        this.id = id;
        this.requestChannel = requestChannel;
    }

    @Override
    public void run() {
        log.info("RequestHandler {} started", id);

        try {
            while (running.get()) {
                try {
                    // Receive request from RequestChannel
                    RequestChannel.Request request = requestChannel.receiveRequest(300);

                    if (request != null) {
                        // Process the request
                        processRequest(request);
                    }
                } catch (InterruptedException e) {
                    log.info("RequestHandler {} interrupted", id);
                    break;
                } catch (Exception e) {
                    log.error("Error processing request in handler {}", id, e);
                }
            }
        } finally {
            log.info("RequestHandler {} stopped", id);
        }
    }

    /**
     * Process a request and send response.
     * This is a simplified echo service for demonstration.
     */
    private void processRequest(RequestChannel.Request request) throws InterruptedException {
        log.debug("Handler {} processing request from connection {}",
                id, request.connectionId);

        try {
            // Read the request data
            ByteBuffer requestBuffer = request.buffer;
            byte[] requestBytes = new byte[requestBuffer.remaining()];
            requestBuffer.get(requestBytes);
            String requestData = new String(requestBytes, StandardCharsets.UTF_8);

            log.info("Handler {} received: '{}' from {}", id, requestData, request.connectionId);

            // Simulate processing time
            Thread.sleep(10);

            // Create response (echo back with prefix)
            String responseData = "ECHO: " + requestData;
            ByteBuffer responseBuffer = ByteBuffer.wrap(responseData.getBytes(StandardCharsets.UTF_8));

            // Send response back through RequestChannel
            RequestChannel.Response response = new RequestChannel.Response(
                    request.processorId,
                    request,
                    responseBuffer
            );

            requestChannel.sendResponse(response);
            log.debug("Handler {} sent response to processor {}", id, request.processorId);

        } catch (Exception e) {
            log.error("Error processing request in handler {}", id, e);
        }
    }

    public void shutdown() {
        running.set(false);
    }
}
