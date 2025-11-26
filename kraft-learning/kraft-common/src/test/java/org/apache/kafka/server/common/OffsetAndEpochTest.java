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
package org.apache.kafka.server.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * OffsetAndEpoch 的单元测试
 *
 * 测试目标：
 * 1. 验证compareTo方法的正确性（epoch优先规则）
 * 2. 验证equals和hashCode的正确性
 * 3. 验证record的基本功能
 */
class OffsetAndEpochTest {

    /**
     * 测试：epoch不同时，epoch大的更大
     *
     * 场景：比较两个offset相同但epoch不同的对象
     * 预期：epoch=6的 > epoch=5的
     */
    @Test
    void testCompareToWithDifferentEpochs() {
        OffsetAndEpoch oe1 = new OffsetAndEpoch(100, 5);  // offset=100, epoch=5
        OffsetAndEpoch oe2 = new OffsetAndEpoch(100, 6);  // offset=100, epoch=6

        // epoch=6 > epoch=5
        assertTrue(oe2.compareTo(oe1) > 0, "Higher epoch should be greater");
        assertTrue(oe1.compareTo(oe2) < 0, "Lower epoch should be smaller");
    }

    /**
     * 测试：epoch不同时，即使offset更小，高epoch的也更大
     *
     * 场景：offset=50但epoch=6 vs offset=100但epoch=5
     * 预期：epoch优先，(50, 6) > (100, 5)
     */
    @Test
    void testCompareToEpochTakesPrecedence() {
        OffsetAndEpoch smallOffsetHighEpoch = new OffsetAndEpoch(50, 6);
        OffsetAndEpoch largeOffsetLowEpoch = new OffsetAndEpoch(100, 5);

        // epoch=6 > epoch=5，即使offset更小
        assertTrue(smallOffsetHighEpoch.compareTo(largeOffsetLowEpoch) > 0,
            "Epoch should take precedence over offset");
    }

    /**
     * 测试：epoch相同时，比较offset
     *
     * 场景：两个对象epoch相同但offset不同
     * 预期：offset大的更大
     */
    @Test
    void testCompareToWithSameEpoch() {
        OffsetAndEpoch oe1 = new OffsetAndEpoch(100, 5);  // offset=100, epoch=5
        OffsetAndEpoch oe2 = new OffsetAndEpoch(200, 5);  // offset=200, epoch=5

        // epoch相同，比较offset
        assertTrue(oe2.compareTo(oe1) > 0, "Higher offset should be greater when epoch is same");
        assertTrue(oe1.compareTo(oe2) < 0, "Lower offset should be smaller when epoch is same");
    }

    /**
     * 测试：完全相同的对象
     *
     * 场景：offset和epoch都相同
     * 预期：compareTo返回0
     */
    @Test
    void testCompareToEqual() {
        OffsetAndEpoch oe1 = new OffsetAndEpoch(100, 5);
        OffsetAndEpoch oe2 = new OffsetAndEpoch(100, 5);

        assertEquals(0, oe1.compareTo(oe2), "Equal objects should return 0");
    }

    /**
     * 测试：equals方法
     *
     * 场景：验证record自动生成的equals方法
     * 预期：offset和epoch都相同时equals返回true
     */
    @Test
    void testEquals() {
        OffsetAndEpoch oe1 = new OffsetAndEpoch(100, 5);
        OffsetAndEpoch oe2 = new OffsetAndEpoch(100, 5);
        OffsetAndEpoch oe3 = new OffsetAndEpoch(100, 6);
        OffsetAndEpoch oe4 = new OffsetAndEpoch(200, 5);

        // 相同的应该equals
        assertEquals(oe1, oe2);

        // 不同的不应该equals
        assertNotEquals(oe1, oe3);  // epoch不同
        assertNotEquals(oe1, oe4);  // offset不同
    }

    /**
     * 测试：hashCode方法
     *
     * 场景：验证record自动生成的hashCode方法
     * 预期：相同对象的hashCode相同
     */
    @Test
    void testHashCode() {
        OffsetAndEpoch oe1 = new OffsetAndEpoch(100, 5);
        OffsetAndEpoch oe2 = new OffsetAndEpoch(100, 5);

        assertEquals(oe1.hashCode(), oe2.hashCode(),
            "Equal objects should have same hashCode");
    }

    /**
     * 测试：getter方法
     *
     * 场景：验证record自动生成的getter方法
     * 预期：offset()和epoch()返回正确的值
     */
    @Test
    void testGetters() {
        OffsetAndEpoch oe = new OffsetAndEpoch(123, 45);

        assertEquals(123, oe.offset(), "offset() should return correct value");
        assertEquals(45, oe.epoch(), "epoch() should return correct epoch");
    }

    /**
     * 测试：toString方法
     *
     * 场景：验证record自动生成的toString方法
     * 预期：包含offset和epoch的信息
     */
    @Test
    void testToString() {
        OffsetAndEpoch oe = new OffsetAndEpoch(100, 5);
        String str = oe.toString();

        // record的toString格式：OffsetAndEpoch[offset=100, epoch=5]
        assertTrue(str.contains("100"), "toString should contain offset");
        assertTrue(str.contains("5"), "toString should contain epoch");
    }

    /**
     * 测试：边界值
     *
     * 场景：测试极端值（Long.MAX_VALUE, Integer.MAX_VALUE）
     * 预期：能正确处理
     */
    @Test
    void testBoundaryValues() {
        OffsetAndEpoch maxOffset = new OffsetAndEpoch(Long.MAX_VALUE, 1);
        OffsetAndEpoch maxEpoch = new OffsetAndEpoch(1, Integer.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, maxOffset.offset());
        assertEquals(Integer.MAX_VALUE, maxEpoch.epoch());

        // 比较：高epoch优先
        assertTrue(maxEpoch.compareTo(maxOffset) > 0);
    }

    /**
     * 测试：负值（虽然实际使用中不应该出现）
     *
     * 场景：测试offset=0和epoch=0（最小合法值）
     * 预期：能正确处理
     */
    @Test
    void testZeroValues() {
        OffsetAndEpoch zero = new OffsetAndEpoch(0, 0);

        assertEquals(0, zero.offset());
        assertEquals(0, zero.epoch());
    }
}
