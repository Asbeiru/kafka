# Kafka Multi-Reactor Network Model Demo

A simplified implementation of Kafka's Multi-Reactor network model for learning purposes. This project mirrors Kafka's core network architecture with 100% alignment on critical components.

## Architecture

```
Client
  ↓
Acceptor (Main Reactor, 1 thread)
  ↓ Round-Robin with backpressure
Processor-0, 1, 2 (Sub Reactors, 3 threads)
  ↓
RequestChannel (shared queue)
  ↓
Handler Pool (8 business threads)
```

### Key Components

1. **Acceptor (Main Reactor)**
   - One thread with one ServerSocketChannel
   - Accepts new connections
   - Assigns to Processors using Round-Robin with mayBlock backpressure

2. **Processor (Sub Reactor)**
   - One Processor = One Thread + One Selector
   - Implements 7-step event loop
   - Manages multiple client connections
   - Uses mute/unmute for request ordering

3. **RequestChannel**
   - Shared by all Processors
   - Three-tier queue structure
   - Coordinates between network and business layers

4. **Handler Pool**
   - Business logic processing threads
   - Pull requests from RequestChannel
   - Process and send responses back

## Critical Implementation Details

All core logic aligns with Kafka source code (SocketServer.scala, Selector.java):

- **Connection queue size**: 20 (matches Kafka's CONNECTION_QUEUE_SIZE)
- **mayBlock backpressure**: N-1 non-blocking tries, last attempt blocks
- **7-step event loop**: configureNewConnections → processNewResponses → poll → processCompletedReceives → processCompletedSends → processDisconnected → closeExcessConnections
- **Message format**: [4-byte size] + [payload]
- **Connection ID format**: remoteHost:remotePort-localPort-index

## Build and Run

### Prerequisites
- Java 11 or higher
- Maven 3.6 or higher

### Build
```bash
cd kafka-reactor-demo
mvn clean package
```

### Run Server
```bash
# Using Maven
mvn exec:java -Dexec.mainClass="com.kafka.reactor.server.SocketServer"

# Or using Java directly
java -cp target/kafka-reactor-demo-1.0-SNAPSHOT.jar com.kafka.reactor.server.SocketServer

# With custom host and port
java -cp target/kafka-reactor-demo-1.0-SNAPSHOT.jar com.kafka.reactor.server.SocketServer localhost 9092
```

### Run Test Client
```bash
# In a separate terminal
mvn exec:java -Dexec.mainClass="com.kafka.reactor.client.SimpleClient"

# Or using Java directly
java -cp target/kafka-reactor-demo-1.0-SNAPSHOT.jar:target/test-classes com.kafka.reactor.client.SimpleClient

# With custom parameters (host, port, numMessages)
java -cp target/kafka-reactor-demo-1.0-SNAPSHOT.jar:target/test-classes com.kafka.reactor.client.SimpleClient localhost 9092 10
```

## Expected Output

### Server Logs
You should observe:
1. Acceptor accepting connections
2. Round-Robin assignment to Processors
3. Processor configuring new connections
4. 7-step event loop execution
5. Request/Response flow through RequestChannel
6. Handler processing requests

Example:
```
12:34:56.123 [acceptor-PLAINTEXT] INFO  Acceptor - Accepted connection from /127.0.0.1:54321
12:34:56.124 [acceptor-PLAINTEXT] DEBUG Acceptor - Assigned connection to processor 0
12:34:56.125 [processor-0] INFO  Processor - Processor 0 accepted connection: 127.0.0.1:54321-9092-0
12:34:56.200 [processor-0] DEBUG Processor - Processor 0 sent request from 127.0.0.1:54321-9092-0 to RequestChannel
12:34:56.201 [handler-3] INFO  RequestHandler - Handler 3 received: 'Hello from client, message #1'
12:34:56.212 [handler-3] DEBUG RequestHandler - Handler 3 sent response to processor 0
12:34:56.213 [processor-0] DEBUG Processor - Processor 0 queued response for 127.0.0.1:54321-9092-0
```

### Client Output
```
Connecting to localhost:9092
Connected to server
Sent: Hello from client, message #1
Received: ECHO: Hello from client, message #1
Sent: Hello from client, message #2
Received: ECHO: Hello from client, message #2
...
```

## Code Review Fixes Applied

This implementation includes all critical fixes from the code review:

1. ✅ **Acceptor mayBlock logic** - Round-Robin with N-1 non-blocking + 1 blocking attempt
2. ✅ **Processor.accept() mayBlock parameter** - Backpressure mechanism implementation
3. ✅ **ArrayBlockingQueue capacity 20** - Matches Kafka's CONNECTION_QUEUE_SIZE
4. ✅ **configureNewConnections batch limit** - Process max 20 connections per iteration
5. ✅ **KafkaSelector.wakeup()** - Selector wakeup mechanism for responsiveness

## Alignment with Kafka Source Code

### Class Mapping

| Our Class | Kafka Source | Location |
|-----------|--------------|----------|
| SocketServer | SocketServer | core/src/main/scala/kafka/network/SocketServer.scala:101 |
| Acceptor | Acceptor | SocketServer.scala:476-785 |
| Processor | Processor | SocketServer.scala:815-1283 |
| RequestChannel | RequestChannel | core/src/main/scala/kafka/network/RequestChannel.scala:344-499 |
| KafkaSelector | Selector | clients/src/main/java/org/apache/kafka/common/network/Selector.java:106 |
| KafkaChannel | KafkaChannel | clients/src/main/java/org/apache/kafka/common/network/KafkaChannel.java:67 |
| NetworkReceive | NetworkReceive | clients/src/main/java/org/apache/kafka/common/network/NetworkReceive.java:30 |
| NetworkSend | NetworkSend | clients/src/main/java/org/apache/kafka/common/network/NetworkSend.java:30 |
| ConnectionQuotas | ConnectionQuotas | SocketServer.scala:189-287 |
| MemoryPool | MemoryPool | clients/src/main/java/org/apache/kafka/common/utils/MemoryPool.java:22 |

## Learning Path

### Stage 1: Basic Understanding (1-2 days)
- Read this README and architecture diagram
- Run server and client, observe logs
- Understand Multi-Reactor pattern basics

### Stage 2: Core Components (3-5 days)
- Study Acceptor → Processor → Handler flow
- Understand 7-step event loop in Processor
- Learn mute/unmute mechanism for ordering
- Trace request lifecycle through logs

### Stage 3: Source Code Alignment (5-7 days)
- Compare implementation with Kafka source
- Study critical sections (mayBlock, event loop, etc.)
- Use provided line number references
- Read the comprehensive analysis documents

### Stage 4: Performance & Advanced Topics (optional)
- Connection quotas and backpressure
- Memory pool CAS operations
- Zero-copy and Scatter/Gather I/O
- Performance tuning parameters

## Related Documentation

See the comprehensive analysis documents in the parent directory:

- **KAFKA_REACTOR_PROJECT_SUMMARY.md** - Complete project summary
- **CODE_REVIEW_KAFKA_REACTOR_IMPLEMENTATION.md** - Code review findings
- **PROCESSOR_THREAD_MODEL_ANALYSIS.md** - Processor thread model deep dive
- **ACCEPTOR_CONNECTION_ASSIGNMENT_ANALYSIS.md** - Connection assignment strategy
- **REQUESTCHANNEL_DEEP_DIVE.md** - RequestChannel architecture
- **REACTOR_PATTERN_DEEP_DIVE.md** - Reactor pattern across systems
- **MULTI_REACTOR_IMPLEMENTATION_GUIDE.md** - Implementation guide
- **KAFKA_NETWORK_MODEL_ANALYSIS.md** - Initial analysis

## License

This is an educational project for learning Kafka's network model. Not for production use.

## References

- [Apache Kafka Source Code](https://github.com/apache/kafka)
- Kafka SocketServer: `core/src/main/scala/kafka/network/SocketServer.scala`
- Kafka Selector: `clients/src/main/java/org/apache/kafka/common/network/Selector.java`
- Reactor Pattern: Douglas Schmidt's C++ Report (1995)
