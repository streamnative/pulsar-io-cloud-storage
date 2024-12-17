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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class BlendBatchMangerTest {

    @Test
    public void testFlushBySize() throws InterruptedException {
        BlendBatchManager blendBatchManger = new BlendBatchManager(1000,
                1000, 1000);
        sendAndVerify(blendBatchManger);
    }

    @Test
    public void testFlushByTimeOut() throws InterruptedException {
        BlendBatchManager blendBatchManger = new BlendBatchManager(10000000,
                100000000, 1000);
        sendAndVerify(blendBatchManger);
    }

    private void sendAndVerify(BlendBatchManager blendBatchManger) throws InterruptedException {
        // Send 10000 records, message size is random
        int numRecords = 10000;
        new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                Random random = new Random();
                for (int i = 0; i < numRecords; i++) {
                    String topicName = "topic-" + i % 10;
                    int size = random.nextInt(10);
                    if (i % 99 == 0) {
                        size += 991;
                    }
                    blendBatchManger.add(getRecord(topicName, size));
                }
            }
        }).start();

        // Poll records
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger receivedRecords = new AtomicInteger();
        new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                while (true) {
                    Map<String, List<Record<GenericRecord>>> records = blendBatchManger.pollNeedFlushData();
                    if (records.isEmpty()) {
                        Thread.sleep(50);
                        continue;
                    }
                    receivedRecords.addAndGet(records.values().stream()
                            .mapToInt(List::size)
                            .sum());
                    if (receivedRecords.get() == numRecords) {
                        break;
                    }
                }
                latch.countDown();
            }
        }).start();
        latch.await();
        assertEquals(receivedRecords.get(), numRecords);
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