/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.raft.RaftMessage;
import org.apache.kafka.raft.RaftMessageQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * BlockingMessageQueue - 基于阻塞队列的Raft消息队列实现
 *
 * 【核心职责】
 * 在不同线程之间安全地传递Raft消息（请求和响应）。
 *
 * 【使用场景】
 *
 * Kafka的KRaft有多个线程协作：
 * 1. 网络线程（Network Thread）
 *    - 接收远程节点的请求
 *    - 将请求放入消息队列
 *
 * 2. Raft主线程（Raft Main Thread）
 *    - 从消息队列取出请求
 *    - 处理Raft逻辑（投票、日志复制等）
 *    - 更新状态机
 *
 * 3. 应用线程（Application Thread）
 *    - 提交写入请求到Raft
 *    - 等待Raft确认
 *
 * <pre>
 * ┌─────────────────┐
 * │  网络线程       │
 * │  (接收请求)     │
 * └────────┬────────┘
 *          │ add(message)
 *          ▼
 * ┌─────────────────┐
 * │  消息队列       │  线程安全
 * │ (BlockingQueue) │  可阻塞等待
 * └────────┬────────┘
 *          │ poll(timeout)
 *          ▼
 * ┌─────────────────┐
 * │  Raft主线程     │
 * │  (处理逻辑)     │
 * └─────────────────┘
 * </pre>
 *
 * 【为什么使用阻塞队列？】
 *
 * 1. 线程安全
 *    - 多个生产者（网络线程、应用线程）并发add()
 *    - 单个消费者（Raft线程）poll()
 *    - BlockingQueue内置同步机制，无需额外加锁
 *
 * 2. 阻塞等待
 *    - Raft线程没有消息时可以阻塞等待
 *    - 避免忙等待（busy-waiting）浪费CPU
 *    - 支持超时等待（poll(timeoutMs)）
 *
 * 3. 背压（Backpressure）
 *    - 可以配置队列容量上限（虽然这里用的是无界队列）
 *    - 队列满时可以阻塞生产者
 *    - 防止消息堆积过多耗尽内存
 *
 * 【WAKEUP机制】
 *
 * 问题场景：
 * <pre>
 * // Raft线程正在等待消息
 * RaftMessage msg = queue.poll(5000);  // 阻塞最多5秒
 *
 * // 此时应用线程想要关闭Raft
 * raft.shutdown();
 *
 * // 问题：Raft线程可能还要等待5秒才能响应关闭请求
 * </pre>
 *
 * 解决方案：WAKEUP_MESSAGE
 * <pre>
 * // 应用线程调用
 * queue.wakeup();  // 插入特殊的WAKEUP_MESSAGE
 *
 * // Raft线程立即从poll()返回
 * RaftMessage msg = queue.poll(5000);  // 返回null（WAKEUP_MESSAGE被过滤）
 *
 * // Raft线程可以立即检查关闭标志并退出
 * if (shuttingDown) {
 *     cleanup();
 *     return;
 * }
 * </pre>
 *
 * 【size计数器】
 *
 * 为什么单独维护size而不是调用queue.size()？
 *
 * 1. 性能考虑
 *    - LinkedBlockingQueue.size()需要遍历整个队列（O(n)）
 *    - AtomicInteger.get()是O(1)操作
 *    - isEmpty()是高频调用，需要高性能
 *
 * 2. 精确语义
 *    - size不包含WAKEUP_MESSAGE
 *    - queue.size()会把WAKEUP_MESSAGE计入
 *    - 应用只关心真实消息的数量
 *
 * 3. 线程安全
 *    - AtomicInteger保证并发更新的正确性
 *    - 配合add()的incrementAndGet()
 *    - 配合poll()的decrementAndGet()
 *
 * 【使用示例】
 *
 * 1. 基本的生产者-消费者模式：
 * <pre>
 * // 初始化
 * BlockingMessageQueue queue = new BlockingMessageQueue();
 *
 * // 网络线程（生产者）：接收到Raft请求
 * void onRequestReceived(RaftRequest.Inbound request) {
 *     queue.add(request);  // 线程安全地添加
 * }
 *
 * // Raft线程（消费者）：处理消息
 * void raftMainLoop() {
 *     while (running) {
 *         RaftMessage message = queue.poll(100);  // 等待最多100ms
 *         if (message != null) {
 *             handleMessage(message);
 *         }
 *         // 即使message为null，也可以做其他工作（如超时检测）
 *     }
 * }
 * </pre>
 *
 * 2. 优雅关闭：
 * <pre>
 * // Raft线程
 * void raftMainLoop() {
 *     while (!shuttingDown) {
 *         RaftMessage message = queue.poll(1000);
 *         if (message != null) {
 *             handleMessage(message);
 *         }
 *         checkTimeouts();  // 定期检查超时
 *     }
 *     cleanup();
 * }
 *
 * // 应用线程：请求关闭
 * void shutdown() {
 *     shuttingDown = true;
 *     queue.wakeup();  // 唤醒Raft线程，使其立即检查shuttingDown标志
 * }
 * </pre>
 *
 * 3. 批量处理：
 * <pre>
 * void processBatch() {
 *     List<RaftMessage> batch = new ArrayList<>();
 *
 *     // 等待第一条消息（可阻塞）
 *     RaftMessage first = queue.poll(100);
 *     if (first != null) {
 *         batch.add(first);
 *
 *         // 立即取出队列中的其他消息（不阻塞）
 *         while (!queue.isEmpty()) {
 *             RaftMessage msg = queue.poll(0);  // 超时0ms，不阻塞
 *             if (msg != null) {
 *                 batch.add(msg);
 *             } else {
 *                 break;
 *             }
 *         }
 *
 *         // 批量处理
 *         handleBatch(batch);
 *     }
 * }
 * </pre>
 *
 * 4. 条件等待：
 * <pre>
 * // Follower等待Leader的响应
 * void waitForLeaderResponse() {
 *     long deadline = System.currentTimeMillis() + 5000;
 *
 *     while (System.currentTimeMillis() < deadline) {
 *         long remainingMs = deadline - System.currentTimeMillis();
 *         RaftMessage msg = queue.poll(remainingMs);
 *
 *         if (msg instanceof FetchResponse) {
 *             handleFetchResponse((FetchResponse) msg);
 *             return;
 *         } else if (msg != null) {
 *             // 其他消息，继续等待
 *             handleOtherMessage(msg);
 *         }
 *     }
 *
 *     // 超时
 *     handleTimeout();
 * }
 * </pre>
 *
 * 【线程安全保证】
 *
 * 1. add()：多个生产者并发调用是安全的
 *    - LinkedBlockingQueue内部使用锁保护
 *    - AtomicInteger.incrementAndGet()是原子操作
 *
 * 2. poll()：单个消费者调用（Raft线程）
 *    - BlockingQueue支持多消费者，但KRaft只用单消费者
 *    - AtomicInteger.decrementAndGet()是原子操作
 *
 * 3. isEmpty()：可以被任何线程调用
 *    - AtomicInteger.get()是无锁的读操作
 *    - 返回值是近似的（可能有消息正在添加中）
 *    - 对于性能监控和调试已经足够准确
 *
 * 4. wakeup()：可以被任何线程调用
 *    - 只是插入一条特殊消息
 *    - LinkedBlockingQueue保证线程安全
 *
 * 【性能特征】
 *
 * 1. 时间复杂度
 *    - add()：O(1)
 *    - poll()：O(1)
 *    - isEmpty()：O(1)
 *    - wakeup()：O(1)
 *
 * 2. 空间复杂度
 *    - 无界队列，理论上可以无限增长
 *    - 实际受限于内存和Raft的处理速度
 *    - 如果消息堆积过多，说明Raft处理速度跟不上
 *
 * 3. 阻塞行为
 *    - add()不阻塞（无界队列）
 *    - poll()可以阻塞等待消息
 *    - 支持超时（避免无限等待）
 *
 * @see RaftMessageQueue 消息队列接口
 * @see RaftMessage 消息接口
 */
public class BlockingMessageQueue implements RaftMessageQueue {
    /**
     * WAKEUP_MESSAGE - 唤醒消息
     *
     * 【目的】
     * 中断Raft线程的poll()阻塞等待。
     *
     * 【实现】
     * 一个特殊的RaftMessage单例：
     * - correlationId() 返回 0
     * - data() 返回 null
     *
     * 【为什么是单例？】
     * - poll()通过 == 比较判断（引用相等）
     * - 不需要创建新对象，节省内存
     * - 线程安全（不可变对象）
     *
     * 【处理方式】
     * poll()收到WAKEUP_MESSAGE时返回null，
     * 不作为真实消息返回给调用者。
     */
    private static final RaftMessage WAKEUP_MESSAGE = new RaftMessage() {
        @Override
        public int correlationId() {
            return 0;
        }

        @Override
        public ApiMessage data() {
            return null;
        }
    };

    /**
     * 内部消息队列
     *
     * 使用LinkedBlockingQueue的原因：
     * 1. 线程安全：内置锁机制，支持多生产者单消费者
     * 2. 阻塞等待：poll()可以阻塞等待消息
     * 3. 无界队列：不限制容量（实际受限于内存）
     * 4. FIFO顺序：保证消息的顺序性
     *
     * 替代方案：
     * - ArrayBlockingQueue：有界，需要指定容量
     * - ConcurrentLinkedQueue：无阻塞，不支持poll(timeout)
     * - SynchronousQueue：容量为0，必须有消费者等待
     */
    private final BlockingQueue<RaftMessage> queue = new LinkedBlockingQueue<>();

    /**
     * 消息计数器
     *
     * 【为什么需要？】
     * 1. queue.size()是O(n)操作（遍历链表）
     * 2. isEmpty()是高频调用，需要O(1)性能
     * 3. WAKEUP_MESSAGE不应该计入真实消息数量
     *
     * 【更新规则】
     * - add(message)：size.incrementAndGet()
     * - poll()返回真实消息：size.decrementAndGet()
     * - poll()返回null或WAKEUP_MESSAGE：size不变
     *
     * 【线程安全】
     * AtomicInteger保证并发更新的正确性。
     */
    private final AtomicInteger size = new AtomicInteger(0);

    /**
     * 从队列中取出消息
     *
     * 【阻塞行为】
     * - 队列为空时，阻塞等待最多timeoutMs毫秒
     * - 收到消息或超时后返回
     *
     * 【返回值】
     * - 真实消息：返回RaftMessage
     * - WAKEUP_MESSAGE：返回null（过滤掉）
     * - 超时：返回null
     *
     * 【异常处理】
     * 如果等待被中断（InterruptedException），
     * 转换为Kafka的InterruptException抛出。
     *
     * 【使用场景】
     * <pre>
     * // Raft主循环
     * while (running) {
     *     RaftMessage msg = queue.poll(100);
     *     if (msg != null) {
     *         if (msg instanceof RaftRequest.Inbound) {
     *             handleRequest((RaftRequest.Inbound) msg);
     *         } else if (msg instanceof RaftResponse.Inbound) {
     *             handleResponse((RaftResponse.Inbound) msg);
     *         }
     *     }
     *     // 即使msg为null，也可以做其他工作
     *     checkTimeouts();
     *     sendHeartbeats();
     * }
     * </pre>
     *
     * @param timeoutMs 超时时间（毫秒），0表示不阻塞，负数表示无限等待
     * @return 消息对象，如果超时或收到WAKEUP则返回null
     * @throws InterruptException 如果等待被中断
     */
    @Override
    public RaftMessage poll(long timeoutMs) {
        try {
            // 从BlockingQueue取消息，可能阻塞
            RaftMessage message = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);

            if (message == null || message == WAKEUP_MESSAGE) {
                // 超时或唤醒消息，返回null
                return null;
            } else {
                // 真实消息，减少计数器
                size.decrementAndGet();
                return message;
            }
        } catch (InterruptedException e) {
            // 等待被中断，转换为Kafka的异常类型
            throw new InterruptException(e);
        }
    }

    /**
     * 添加消息到队列
     *
     * 【非阻塞】
     * 因为使用无界队列，add()永远不会阻塞。
     *
     * 【线程安全】
     * LinkedBlockingQueue内部使用锁保护，
     * 多个线程可以并发调用add()。
     *
     * 【计数更新】
     * 增加size计数器，追踪真实消息数量。
     *
     * 【使用场景】
     * <pre>
     * // 网络线程接收到投票请求
     * void onVoteRequestReceived(RaftRequest.Inbound request) {
     *     // 线程安全地添加到队列
     *     messageQueue.add(request);
     *
     *     // Raft线程会在poll()中取到这条消息
     * }
     *
     * // 应用线程提交写入
     * void appendRecord(byte[] data) {
     *     RaftRequest.Inbound request = createAppendRequest(data);
     *     messageQueue.add(request);
     *
     *     // 等待Raft处理并响应
     *     waitForCompletion();
     * }
     * </pre>
     *
     * @param message 要添加的消息（不能为null）
     */
    @Override
    public void add(RaftMessage message) {
        queue.add(message);
        size.incrementAndGet();
    }

    /**
     * 检查队列是否为空
     *
     * 【性能】
     * O(1)操作，通过AtomicInteger.get()实现。
     *
     * 【语义】
     * 返回true表示没有真实消息。
     * 队列中可能有WAKEUP_MESSAGE，但不计入size。
     *
     * 【近似性】
     * 在并发环境下，返回值是近似的：
     * - 返回true后，立即可能有消息被添加
     * - 返回false后，立即可能有消息被取出
     *
     * 这种近似性对于性能监控和调试是可接受的。
     *
     * 【使用场景】
     * <pre>
     * // 监控消息堆积
     * if (!queue.isEmpty()) {
     *     log.warn("Message queue has {} pending messages", queue.size());
     * }
     *
     * // 批量处理
     * List<RaftMessage> batch = new ArrayList<>();
     * batch.add(queue.poll(100));  // 等待第一条
     * while (!queue.isEmpty()) {
     *     batch.add(queue.poll(0));  // 立即取出其他消息
     * }
     * </pre>
     *
     * @return true表示队列中没有真实消息
     */
    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    /**
     * 唤醒阻塞在poll()的线程
     *
     * 【机制】
     * 插入特殊的WAKEUP_MESSAGE到队列。
     * poll()会立即返回null（不会返回WAKEUP_MESSAGE）。
     *
     * 【使用场景】
     *
     * 1. 优雅关闭：
     * <pre>
     * // Raft线程
     * while (!shuttingDown) {
     *     RaftMessage msg = queue.poll(5000);
     *     if (msg != null) handleMessage(msg);
     * }
     *
     * // 应用线程
     * void shutdown() {
     *     shuttingDown = true;
     *     queue.wakeup();  // 立即唤醒Raft线程
     * }
     * </pre>
     *
     * 2. 配置变更：
     * <pre>
     * // 更新配置
     * void updateConfig(RaftConfig newConfig) {
     *     this.config = newConfig;
     *     queue.wakeup();  // 唤醒Raft线程应用新配置
     * }
     *
     * // Raft线程
     * RaftMessage msg = queue.poll(timeout);
     * if (msg == null) {
     *     // 可能是wakeup()调用，检查配置是否变更
     *     applyConfigIfChanged();
     * }
     * </pre>
     *
     * 3. 强制超时检测：
     * <pre>
     * // 定时器线程
     * void onTimerFired() {
     *     queue.wakeup();  // 唤醒Raft线程检查超时
     * }
     *
     * // Raft线程
     * RaftMessage msg = queue.poll(Long.MAX_VALUE);  // 几乎无限等待
     * if (msg == null) {
     *     // wakeup()被调用，检查超时
     *     checkAndHandleTimeouts();
     * }
     * </pre>
     *
     * 【线程安全】
     * 可以被任何线程调用，LinkedBlockingQueue保证线程安全。
     *
     * 【幂等性】
     * 多次调用wakeup()只会插入多条WAKEUP_MESSAGE，
     * 都会被poll()过滤掉，不影响正确性。
     */
    @Override
    public void wakeup() {
        queue.add(WAKEUP_MESSAGE);
    }
}
