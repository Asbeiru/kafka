package com.kafka.reactor.quota;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection quota management with three levels:
 * 1. Broker-level: Total connections across all listeners
 * 2. Listener-level: Total connections for a specific listener
 * 3. IP-level: Connections per IP address
 *
 * Aligns with Kafka's ConnectionQuotas class.
 */
public class ConnectionQuotas {
    private static final Logger log = LoggerFactory.getLogger(ConnectionQuotas.class);

    private final int maxConnectionsPerIp;
    private final int maxConnectionsPerIpOverrides;
    private final int maxConnections;

    // Broker-level total
    private final AtomicInteger totalCount = new AtomicInteger(0);

    // Listener-level counts
    private final Map<String, AtomicInteger> listenerCounts = new ConcurrentHashMap<>();

    // IP-level counts
    private final Map<InetAddress, AtomicInteger> ipCounts = new ConcurrentHashMap<>();

    public ConnectionQuotas(int maxConnectionsPerIp, int maxConnections) {
        this.maxConnectionsPerIp = maxConnectionsPerIp;
        this.maxConnectionsPerIpOverrides = 0;
        this.maxConnections = maxConnections;
    }

    /**
     * Increment connection count for a client.
     * Returns true if connection is accepted, false if rejected due to quota.
     */
    public synchronized boolean inc(String listenerName, InetAddress address) {
        // Check broker-level quota
        if (maxConnections > 0 && totalCount.get() >= maxConnections) {
            log.warn("Broker connection limit reached: {}/{}", totalCount.get(), maxConnections);
            return false;
        }

        // Check IP-level quota
        AtomicInteger count = ipCounts.computeIfAbsent(address, k -> new AtomicInteger(0));
        if (maxConnectionsPerIp > 0 && count.get() >= maxConnectionsPerIp) {
            log.warn("IP connection limit reached for {}: {}/{}", address, count.get(), maxConnectionsPerIp);
            return false;
        }

        // Accept connection - increment all counters
        count.incrementAndGet();
        totalCount.incrementAndGet();
        listenerCounts.computeIfAbsent(listenerName, k -> new AtomicInteger(0)).incrementAndGet();

        log.debug("Connection accepted from {} on listener {}. Total: {}, IP count: {}",
                address, listenerName, totalCount.get(), count.get());
        return true;
    }

    /**
     * Decrement connection count when a client disconnects.
     */
    public synchronized void dec(String listenerName, InetAddress address) {
        // Decrement IP count
        AtomicInteger count = ipCounts.get(address);
        if (count != null) {
            int newCount = count.decrementAndGet();
            if (newCount == 0) {
                ipCounts.remove(address);
            }
        }

        // Decrement listener count
        AtomicInteger listenerCount = listenerCounts.get(listenerName);
        if (listenerCount != null) {
            listenerCount.decrementAndGet();
        }

        // Decrement total count
        totalCount.decrementAndGet();

        log.debug("Connection closed from {} on listener {}. Total: {}",
                address, listenerName, totalCount.get());
    }

    /**
     * Get current connection count for an IP.
     */
    public int get(InetAddress address) {
        AtomicInteger count = ipCounts.get(address);
        return count != null ? count.get() : 0;
    }

    /**
     * Get total connection count.
     */
    public int totalCount() {
        return totalCount.get();
    }

    /**
     * Get connection count for a listener.
     */
    public int listenerCount(String listenerName) {
        AtomicInteger count = listenerCounts.get(listenerName);
        return count != null ? count.get() : 0;
    }

    @Override
    public String toString() {
        return "ConnectionQuotas{" +
                "maxConnectionsPerIp=" + maxConnectionsPerIp +
                ", maxConnections=" + maxConnections +
                ", totalCount=" + totalCount.get() +
                ", listenerCounts=" + listenerCounts +
                ", ipCounts=" + ipCounts +
                '}';
    }
}
