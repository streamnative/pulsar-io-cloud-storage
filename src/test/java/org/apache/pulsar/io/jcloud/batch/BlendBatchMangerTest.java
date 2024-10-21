/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.jcloud.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.Test;

public class BlendBatchMangerTest {

    public void test(long maxBatchSize, long maxBatchBytes, int maxPendingQueueSize) throws InterruptedException {
        BlendBatchManager blendBatchManger = new BlendBatchManager(maxBatchSize,
                maxBatchBytes, 10000, maxPendingQueueSize);

        for (int i = 0; i < 15; i++) {
            if (i % 2 == 0) {
                blendBatchManger.add(getRecord("topic-0", 2));
            } else {
                blendBatchManger.add(getRecord("topic-1", 2));
            }
        }

        // assert size and bytes
        assertEquals(15, blendBatchManger.getCurrentBatchSize(null));
        assertEquals(30, blendBatchManger.getCurrentBatchBytes(null));

        // assert trigger flush, and each topic records num is 5
        assertTrue(blendBatchManger.needFlush());
        Map<String, List<Record<GenericRecord>>> flushData = blendBatchManger.pollNeedFlushData();
        assertEquals(2, flushData.size());
        assertEquals(5, flushData.get("topic-0").size());
        assertEquals(5, flushData.get("topic-1").size());
        assertFalse(blendBatchManger.isEmpty());
        assertEquals(5, blendBatchManger.getCurrentBatchSize(null));
        assertEquals(10, blendBatchManger.getCurrentBatchBytes(null));

        // assert not need flush
        assertFalse(blendBatchManger.needFlush());
        assertFalse(blendBatchManger.isEmpty());
    }

    @Test
    public void testFlushBySize() throws InterruptedException {
        test(10, 10000, 1000);
    }

    @Test
    public void testFlushByByteSize() throws InterruptedException {
        test(10000, 20, 1000);
    }

    @Test
    public void testFlushByTimout() throws InterruptedException {
        long maxBatchTimeout = 1000;
        BlendBatchManager blendBatchManger = new BlendBatchManager(1000,
                1000, maxBatchTimeout, 1000);

        blendBatchManger.add(getRecord("topic-0", 2));
        blendBatchManger.add(getRecord("topic-1", 2));
        assertEquals(2, blendBatchManger.getCurrentBatchSize(null));
        assertEquals(4, blendBatchManger.getCurrentBatchBytes(null));
        Thread.sleep(maxBatchTimeout + 100);

        // Time out flush
        Map<String, List<Record<GenericRecord>>> flushData = blendBatchManger.pollNeedFlushData();
        assertEquals(2, flushData.size());
        assertEquals(1, flushData.get("topic-0").size());
        assertEquals(1, flushData.get("topic-1").size());
        assertTrue(blendBatchManger.isEmpty());
        assertEquals(0, blendBatchManger.getCurrentBatchSize(null));
        assertEquals(0, blendBatchManger.getCurrentBatchBytes(null));

        // Time out again
        Thread.sleep(maxBatchTimeout + 100);
        assertTrue(blendBatchManger.pollNeedFlushData().isEmpty());
    }

    Record<GenericRecord> getRecord(String topicName, int size) {
        Message msg = mock(Message.class);
        when(msg.size()).thenReturn(size);
        Record<GenericRecord> mockRecord = mock(Record.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topicName));
        when(mockRecord.getMessage()).thenReturn(Optional.of(msg));
        return mockRecord;
    }

}