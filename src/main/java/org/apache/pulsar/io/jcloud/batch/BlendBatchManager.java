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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * BlendBatchManager is a type of BatchManager that uses a single BatchContainer
 * for all topics. This means that all records, regardless of topic, are batched together.
 */
@Slf4j
public class BlendBatchManager implements BatchManager {

    private final BatchContainer batchContainer;

    public BlendBatchManager(long maxBatchSize, long maxBatchBytes, long maxBatchTimeMs, int maxPendingQueueSize) {
        batchContainer = new BatchContainer(maxBatchSize, maxBatchBytes, maxBatchTimeMs, maxPendingQueueSize);
    }

    @Override
    public void add(Record<GenericRecord> record) throws InterruptedException {
        batchContainer.add(record);
    }

    @Override
    public long getCurrentBatchSize(String topicName) {
        return batchContainer.getCurrentBatchSize();
    }

    @Override
    public long getCurrentBatchBytes(String topicName) {
        return batchContainer.getCurrentBatchBytes();
    }

    @Override
    public Map<String, Tuple2<Long, Long>> getCurrentStats() {
        return Map.of("ALL", Tuples.of(batchContainer.getCurrentBatchSize(), batchContainer.getCurrentBatchBytes()));
    }

    @Override
    public void updateCurrentBatchSize(String topicName, long delta) {
        batchContainer.updateCurrentBatchSize(delta);
    }

    @Override
    public void updateCurrentBatchBytes(String topicName, long delta) {
        batchContainer.updateCurrentBatchBytes(delta);
    }

    @Override
    public boolean isEmpty() {
        return batchContainer.isEmpty();
    }

    @Override
    public boolean needFlush() {
        return batchContainer.needFlush();
    }

    public Map<String, List<Record<GenericRecord>>> pollNeedFlushData() {
        if (!needFlush()) {
            return Map.of();
        }
        List<Record<GenericRecord>> records = batchContainer.pollNeedFlushRecords();
        return records.stream().collect(Collectors.groupingBy(record -> record.getTopicName().get()));
    }
}
