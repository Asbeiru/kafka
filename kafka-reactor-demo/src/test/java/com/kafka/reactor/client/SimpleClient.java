package com.kafka.reactor.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

/**
 * Simple test client to interact with the SocketServer.
 * Sends messages and receives echo responses.
 */
public class SimpleClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9092;
        int numMessages = args.length > 2 ? Integer.parseInt(args[2]) : 5;

        System.out.println("Connecting to " + host + ":" + port);

        // Connect to server
        SocketChannel channel = SocketChannel.open();
        channel.connect(new InetSocketAddress(host, port));
        channel.configureBlocking(true);

        System.out.println("Connected to server");

        // Send multiple messages
        for (int i = 1; i <= numMessages; i++) {
            String message = "Hello from client, message #" + i;

            // Send message with Kafka format: [4-byte size] + [payload]
            sendMessage(channel, message);
            System.out.println("Sent: " + message);

            // Receive response
            String response = receiveMessage(channel);
            System.out.println("Received: " + response);

            // Sleep between messages
            Thread.sleep(1000);
        }

        // Close connection
        channel.close();
        System.out.println("Connection closed");
    }

    /**
     * Send message with Kafka format: [4-byte size] + [payload]
     */
    private static void sendMessage(SocketChannel channel, String message) throws IOException {
        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);

        // Create size header (4 bytes)
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(messageBytes.length);
        sizeBuffer.flip();

        // Create payload buffer
        ByteBuffer payloadBuffer = ByteBuffer.wrap(messageBytes);

        // Write size + payload
        channel.write(sizeBuffer);
        channel.write(payloadBuffer);
    }

    /**
     * Receive message with Kafka format: [4-byte size] + [payload]
     */
    private static String receiveMessage(SocketChannel channel) throws IOException {
        // Read 4-byte size header
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        readFully(channel, sizeBuffer);
        sizeBuffer.flip();
        int size = sizeBuffer.getInt();

        // Read payload
        ByteBuffer payloadBuffer = ByteBuffer.allocate(size);
        readFully(channel, payloadBuffer);
        payloadBuffer.flip();

        byte[] responseBytes = new byte[payloadBuffer.remaining()];
        payloadBuffer.get(responseBytes);

        return new String(responseBytes, StandardCharsets.UTF_8);
    }

    /**
     * Read until buffer is full.
     */
    private static void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0) {
                throw new IOException("Connection closed");
            }
        }
    }
}
