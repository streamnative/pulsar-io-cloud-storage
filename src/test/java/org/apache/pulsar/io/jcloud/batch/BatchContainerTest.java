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
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.Before;
import org.junit.Test;

public class BatchContainerTest {

    private BatchContainer batchContainer;
    private static final long MAX_BATCH_SIZE = 5;
    private static final long MAX_BATCH_BYTES = 100;
    private static final long MAX_BATCH_TIME_MS = 1000;

    @Before
    public void setUp() {
        batchContainer = new BatchContainer(MAX_BATCH_SIZE, MAX_BATCH_BYTES, MAX_BATCH_TIME_MS, 10);
    }

    @Test
    public void testAddAndFlushBySize() throws InterruptedException {
        for (int i = 0; i < MAX_BATCH_SIZE; i++) {
            batchContainer.add(createMockRecord(10));
        }
        assertTrue(batchContainer.needFlush());
        List<Record<GenericRecord>> records = batchContainer.pollNeedFlushRecords();
        assertEquals(MAX_BATCH_SIZE, records.size());
        assertTrue(batchContainer.isEmpty());
    }

    @Test
    public void testAddAndFlushByBytes() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            batchContainer.add(createMockRecord(40));
        }
        assertTrue(batchContainer.needFlush());
        List<Record<GenericRecord>> records = batchContainer.pollNeedFlushRecords();
        assertEquals(3, records.size());
        assertTrue(batchContainer.isEmpty());
    }

    @Test
    public void testFlushByTime() throws InterruptedException {
        batchContainer.add(createMockRecord(10));
        Thread.sleep(MAX_BATCH_TIME_MS + 100); // Wait longer than maxBatchTimeMs
        assertTrue(batchContainer.needFlush());
        List<Record<GenericRecord>> records = batchContainer.pollNeedFlushRecords();
        assertEquals(1, records.size());
        assertTrue(batchContainer.isEmpty());
    }

    @Test
    public void testPollData() throws InterruptedException {
        batchContainer.add(createMockRecord(1));
        assertFalse(batchContainer.needFlush());
        List<Record<GenericRecord>> records = batchContainer.pollNeedFlushRecords();
        assertEquals(1, records.size());
        assertTrue(batchContainer.isEmpty());
    }

    Record<GenericRecord> createMockRecord(int size) {
        Message msg = mock(Message.class);
        when(msg.size()).thenReturn(size);
        Record<GenericRecord> mockRecord = mock(Record.class);
        when(mockRecord.getMessage()).thenReturn(Optional.of(msg));
        return mockRecord;
    }
}
