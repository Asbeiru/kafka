package com.kafka.reactor.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Request channel for passing requests/responses between Processors and Handlers.
 * All Processors share a single RequestChannel instance.
 *
 * Queue structure:
 * - requestQueue: Shared queue for all incoming requests (capacity: queueSize)
 * - responseQueue: Per-Processor queue for responses (one queue per Processor)
 *
 * Aligns with Kafka's RequestChannel class.
 */
public class RequestChannel {
    private static final Logger log = LoggerFactory.getLogger(RequestChannel.class);

    private final int queueSize;
    private final BlockingQueue<Request> requestQueue;
    private final Map<Integer, BlockingQueue<Response>> responseQueues;

    public RequestChannel(int queueSize) {
        this.queueSize = queueSize;
        this.requestQueue = new ArrayBlockingQueue<>(queueSize);
        this.responseQueues = new ConcurrentHashMap<>();
    }

    /**
     * Send a request to the request queue.
     * Called by Processor when it receives a complete request.
     */
    public void sendRequest(Request request) throws InterruptedException {
        requestQueue.put(request);
        log.debug("Request enqueued from processor {}, connection {}, queue size: {}",
                request.processorId, request.connectionId, requestQueue.size());
    }

    /**
     * Receive a request from the request queue.
     * Called by Handler threads to get work.
     */
    public Request receiveRequest(long timeoutMs) throws InterruptedException {
        if (timeoutMs > 0) {
            return requestQueue.poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        } else {
            return requestQueue.take();
        }
    }

    /**
     * Send a response to the appropriate Processor's response queue.
     * Called by Handler after processing a request.
     */
    public void sendResponse(Response response) throws InterruptedException {
        BlockingQueue<Response> responseQueue = responseQueues.get(response.processorId);
        if (responseQueue != null) {
            responseQueue.put(response);
            log.debug("Response enqueued to processor {}, connection {}",
                    response.processorId, response.request.connectionId);
        } else {
            log.warn("Response queue not found for processor {}", response.processorId);
        }
    }

    /**
     * Receive a response from this Processor's response queue.
     * Called by Processor to get responses to send back to clients.
     */
    public Response receiveResponse(int processorId) {
        BlockingQueue<Response> responseQueue = responseQueues.get(processorId);
        if (responseQueue != null) {
            return responseQueue.poll();
        }
        return null;
    }

    /**
     * Add a Processor's response queue.
     * Called when a Processor starts.
     */
    public void addResponseQueue(int processorId) {
        responseQueues.putIfAbsent(processorId, new ArrayBlockingQueue<>(queueSize));
        log.info("Added response queue for processor {}", processorId);
    }

    /**
     * Remove a Processor's response queue.
     * Called when a Processor stops.
     */
    public void removeResponseQueue(int processorId) {
        responseQueues.remove(processorId);
        log.info("Removed response queue for processor {}", processorId);
    }

    public int requestQueueSize() {
        return requestQueue.size();
    }

    /**
     * Request object containing request data and metadata.
     */
    public static class Request {
        public final int processorId;
        public final String connectionId;
        public final ByteBuffer buffer;
        public final long receivedTimeMs;

        public Request(int processorId, String connectionId, ByteBuffer buffer) {
            this.processorId = processorId;
            this.connectionId = connectionId;
            this.buffer = buffer;
            this.receivedTimeMs = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return "Request{" +
                    "processorId=" + processorId +
                    ", connectionId='" + connectionId + '\'' +
                    ", size=" + (buffer != null ? buffer.remaining() : 0) +
                    '}';
        }
    }

    /**
     * Response object containing response data and routing information.
     */
    public static class Response {
        public final int processorId;
        public final Request request;
        public final ByteBuffer buffer;

        public Response(int processorId, Request request, ByteBuffer buffer) {
            this.processorId = processorId;
            this.request = request;
            this.buffer = buffer;
        }

        @Override
        public String toString() {
            return "Response{" +
                    "processorId=" + processorId +
                    ", connectionId='" + request.connectionId + '\'' +
                    ", size=" + (buffer != null ? buffer.remaining() : 0) +
                    '}';
        }
    }
}
