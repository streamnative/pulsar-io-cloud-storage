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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * PartitionedBatchManager is a type of BatchManager that uses separate BatchContainers
 * for each topic. This means that records are batched separately for each topic.
 */
public class PartitionedBatchManager implements BatchManager {

    private final long maxBatchSize;
    private final long maxBatchBytes;
    private final long maxBatchTimeMs;
    private final Map<String, BatchContainer> topicBatchContainer;

    public PartitionedBatchManager(long maxBatchSize, long maxBatchBytes,
                                   long maxBatchTimeMs) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchBytes = maxBatchBytes;
        this.maxBatchTimeMs = maxBatchTimeMs;
        this.topicBatchContainer = new ConcurrentHashMap<>();
    }

    @Override
    public void add(Record<GenericRecord> record) throws InterruptedException {
        String topicName = record.getTopicName()
                .orElseThrow(() -> new IllegalArgumentException("Topic name cannot be null"));
        getBatchContainer(topicName).add(record);
    }

    @Override
    public long getCurrentBatchSize(String topicName) {
        return getBatchContainer(topicName).getCurrentBatchSize();
    }

    @Override
    public long getCurrentBatchBytes(String topicName) {
        return getBatchContainer(topicName).getCurrentBatchBytes();
    }

    @Override
    public Map<String, Tuple2<Long, Long>> getCurrentStats() {
        Map<String, Tuple2<Long, Long>> stats = new HashMap<>();
        topicBatchContainer.forEach((topicName, batchContainer) -> {
            long currentBatchSize = batchContainer.getCurrentBatchSize();
            long currentBatchBytes = batchContainer.getCurrentBatchBytes();
            stats.put(topicName, Tuples.of(currentBatchSize, currentBatchBytes));
        });
        return stats;
    }

    @Override
    public Map<String, List<Record<GenericRecord>>> pollNeedFlushData() {
        return topicBatchContainer.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), entry.getValue().pollNeedFlushRecords()))
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private BatchContainer getBatchContainer(String topicName) {
        return topicBatchContainer.computeIfAbsent(topicName,
                k -> new BatchContainer(maxBatchSize, maxBatchBytes, maxBatchTimeMs));
    }
}
