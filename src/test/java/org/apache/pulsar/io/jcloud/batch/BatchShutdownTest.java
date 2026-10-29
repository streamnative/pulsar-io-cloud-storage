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

import static org.apache.pulsar.io.jcloud.schema.TopicSchemaFallbackTest.TOPIC;
import static org.apache.pulsar.io.jcloud.schema.TopicSchemaFallbackTest.bytes;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.junit.Test;

public class BatchShutdownTest {
    @Test
    public void closeWakesBlockedAddAndDrainsExactlyOnce() throws Exception {
        for (BatchModel model : BatchModel.values()) {
            BlobStoreAbstractConfig config = new BlobStoreAbstractConfig().setBatchSize(1).setBatchTimeMs(60000)
                    .setBatchModel(model);
            BatchManager manager = BatchManager.createBatchManager(config);
            Record<GenericRecord> record = bytes(TOPIC, new byte[]{1});
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<?> producer = executor.submit(() -> {
                    try {
                        manager.add(record);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                await().atMost(Duration.ofSeconds(5)).until(() -> manager.getCurrentBatchSize(TOPIC) == 1);
                Map<String, List<Record<GenericRecord>>> drained = manager.closeAndDrain();
                assertEquals(List.of(record), drained.get(TOPIC));
                producer.get(5, TimeUnit.SECONDS);
                assertEquals(Map.of(), manager.closeAndDrain());
                assertThrows(IllegalStateException.class, () -> manager.add(record));
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
