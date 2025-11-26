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
package org.apache.kafka.raft;

/**
 * OffsetMetadata - 偏移量元数据标记接口
 *
 * 这是一个标记接口（Marker Interface），用于封装偏移量相关的不透明元数据。
 *
 * 【设计目的】
 * 1. 提供扩展点：允许不同的日志实现添加自己的元数据
 * 2. 类型安全：通过接口类型确保元数据的正确使用
 * 3. 解耦设计：Raft核心不需要知道元数据的具体内容
 *
 * 【使用场景】
 * 具体的日志实现（如KafkaMetadataLog）会提供此接口的实现，包含：
 * - 段文件的基础偏移量（segment base offset）
 * - 在段文件中的相对位置（relative position in segment）
 * - 其他日志实现特定的元数据
 *
 * 【为什么是空接口？】
 * 这是一种常见的设计模式：
 * 1. Raft层：只需要知道"有元数据"，不需要知道具体是什么
 * 2. 日志层：提供具体的元数据实现，包含实际的物理位置信息
 * 3. 好处：Raft核心代码与具体的日志存储实现解耦
 *
 * 【实际应用】
 * 在LogOffsetMetadata中使用：
 * <pre>
 * public class LogOffsetMetadata {
 *     private final long offset;                      // 逻辑偏移量
 *     private final Optional<OffsetMetadata> metadata; // 物理元数据（可选）
 * }
 * </pre>
 *
 * 有了物理元数据后，可以：
 * - 快速定位到日志文件的具体位置，无需反复查找
 * - 优化读取性能，避免重复计算
 * - 支持更高效的日志截断和清理操作
 *
 * 【示例实现】
 * 具体实现可能包含：
 * <pre>
 * class KafkaOffsetMetadata implements OffsetMetadata {
 *     private final long segmentBaseOffset;        // 段文件起始偏移量
 *     private final int relativePositionInSegment; // 在段中的位置（字节）
 * }
 * </pre>
 */
// Opaque metadata type which should be instantiated by the log implementation
// 不透明的元数据类型，应该由日志实现来实例化
public interface OffsetMetadata {
}
