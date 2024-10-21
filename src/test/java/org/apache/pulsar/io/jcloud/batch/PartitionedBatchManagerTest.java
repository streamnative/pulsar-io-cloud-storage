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

public class PartitionedBatchManagerTest {

    public void test(long maxBatchSize, long maxBatchBytes, int maxPendingQueueSize) throws InterruptedException {
        PartitionedBatchManager partitionedBatchManager =
                new PartitionedBatchManager(maxBatchSize, maxBatchBytes, 10000, maxPendingQueueSize);

        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                partitionedBatchManager.add(getRecord("topic-0", 2));
            } else {
                partitionedBatchManager.add(getRecord("topic-1", 2));
            }
        }
        // assert not trigger flush by each topic records.
        assertFalse(partitionedBatchManager.needFlush());
        Map<String, List<Record<GenericRecord>>> flushData = partitionedBatchManager.pollNeedFlushData();
        assertEquals(0, flushData.size());

        // add more 5 records to topic-0, then trigger flush.
        for (int i = 0; i < 5; i++) {
            partitionedBatchManager.add(getRecord("topic-0", 2));
        }
        assertTrue(partitionedBatchManager.needFlush());
        flushData = partitionedBatchManager.pollNeedFlushData();
        assertEquals(1, flushData.size());
        assertEquals(10, flushData.get("topic-0").size());

        // assert topic-0 currentBatchSize and currentBatchBytes
        assertEquals(0, partitionedBatchManager.getCurrentBatchSize("topic-0"));
        assertEquals(0, partitionedBatchManager.getCurrentBatchBytes("topic-0"));

        // assert topic-1 currentBatchSize and currentBatchBytes
        assertEquals(5, partitionedBatchManager.getCurrentBatchSize("topic-1"));
        assertEquals(10, partitionedBatchManager.getCurrentBatchBytes("topic-1"));

        // assert not need flush
        assertFalse(partitionedBatchManager.needFlush());
        assertFalse(partitionedBatchManager.isEmpty());
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
        long maxBatchTimeout = 2000;
        PartitionedBatchManager partitionedBatchManager = new PartitionedBatchManager(1000,
                100, maxBatchTimeout, 1000);

        // 1. Add and assert status
        partitionedBatchManager.add(getRecord("topic-0", 2));
        partitionedBatchManager.add(getRecord("topic-1", 101));

        // 2. First sleep maxBatchTimeout / 2
        Thread.sleep(maxBatchTimeout / 2);

        // 3. Poll flush data, assert topic-1 data
        Map<String, List<Record<GenericRecord>>> flushData = partitionedBatchManager.pollNeedFlushData();
        assertEquals(1, flushData.size());
        assertFalse(flushData.containsKey("topic-0"));
        assertEquals(1, flushData.get("topic-1").size());

        // 4. write topic-1 data again, assert not need flush
        partitionedBatchManager.add(getRecord("topic-1", 2));
        // Second sleep maxBatchTimeout / 2
        Thread.sleep(maxBatchTimeout / 2 + 100);

        // 5. assert topic-0 message timeout
        flushData = partitionedBatchManager.pollNeedFlushData();
        assertEquals(1, flushData.size());
        assertEquals(1, flushData.get("topic-0").size());
        assertFalse(flushData.containsKey("topic-1"));

        // 6. Sleep assert can get topic-1 data
        Thread.sleep(maxBatchTimeout / 2 + 100);
        flushData = partitionedBatchManager.pollNeedFlushData();
        assertEquals(1, flushData.size());
        assertFalse(flushData.containsKey("topic-0"));
        assertEquals(1, flushData.get("topic-1").size());
        assertTrue(partitionedBatchManager.isEmpty());

        // Sleep and trigger timeout, and assert not data need flush
        Thread.sleep(maxBatchTimeout + 100);
        assertTrue(partitionedBatchManager.pollNeedFlushData().isEmpty());
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