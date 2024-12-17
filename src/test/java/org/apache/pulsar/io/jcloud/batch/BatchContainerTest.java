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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class BatchContainerTest {

    @Test
    public void testAddAndFlushBySize() throws InterruptedException {
        int maxSize = 10;
        int maxBytes = 1000;
        int maxTimeOut = 999999999;
        BatchContainer batchContainer = new BatchContainer(maxSize, maxBytes, maxTimeOut);
        CountDownLatch latch = new CountDownLatch(1);
        int numRecords = 100;

        Thread addThread = new Thread(() -> {
            try {
                for (int i = 0; i < numRecords; i++) {
                    batchContainer.add(createMockRecord(1));
                }
                latch.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        addThread.start();
        assertFalse(latch.await(500, TimeUnit.MILLISECONDS));

        for (int i = 0; i < numRecords / maxSize; i++) {
            List<Record<GenericRecord>> records = waitForFlush(batchContainer);
            assertEquals(records.size(), maxSize);
        }
        assertTrue(latch.await(500, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testAddAndFlushByBytesAndTimeOut() throws InterruptedException {
        int maxSize = 1000;
        int maxBytes = 100;
        int maxTimeOut = 2000;
        int perRecordSize = 8;
        BatchContainer batchContainer = new BatchContainer(maxSize, maxBytes, maxTimeOut);
        CountDownLatch latch = new CountDownLatch(1);
        int numRecords = 100;
        Thread addThread = new Thread(() -> {
            try {
                for (int i = 0; i < numRecords; i++) {
                    batchContainer.add(createMockRecord(perRecordSize));
                }
                latch.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        addThread.start();
        assertFalse(latch.await(500, TimeUnit.MILLISECONDS));

        AtomicInteger receivedRecords = new AtomicInteger();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> {
                    List<Record<GenericRecord>> records = waitForFlush(batchContainer);
                    receivedRecords.addAndGet(records.size());
                    assertEquals(receivedRecords.get(), numRecords);
                }
        );
        assertTrue(latch.await(500, TimeUnit.MILLISECONDS));
    }

    Record<GenericRecord> createMockRecord(int size) {
        Message msg = mock(Message.class);
        when(msg.size()).thenReturn(size);
        Record<GenericRecord> mockRecord = mock(Record.class);
        when(mockRecord.getMessage()).thenReturn(Optional.of(msg));
        return mockRecord;
    }

    private List<Record<GenericRecord>> waitForFlush(BatchContainer container) throws InterruptedException {
        List<Record<GenericRecord>> records;
        do {
            records = container.pollNeedFlushRecords();
            if (records.isEmpty()) {
                Thread.sleep(50); // Wait a bit before trying again
            }
        } while (records.isEmpty());
        return records;
    }
}
