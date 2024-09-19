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
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * PartitionedBatchManager is a type of BatchManager that uses separate BatchContainers
 * for each topic. This means that records are batched separately for each topic.
 */
public class PartitionedBatchManager implements BatchManager {

    private final long maxBatchSize;
    private final long maxBatchBytes;
    private final int maxPendingQueueSize;
    private final Map<String, BatchContainer> topicBatchContainer;

    public PartitionedBatchManager(long maxBatchSize, long maxBatchBytes, int maxPendingQueueSize) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchBytes = maxBatchBytes;
        this.maxPendingQueueSize = maxPendingQueueSize;
        this.topicBatchContainer = new ConcurrentHashMap<>();
    }

    public void add(Record<GenericRecord> record) throws InterruptedException {
        String topicName = record.getTopicName()
                .orElseThrow(() -> new IllegalArgumentException("Topic name cannot be null"));
        getBatchContainer(topicName).add(record);
    }

    public long getCurrentBatchSize(String topicName) {
        return getBatchContainer(topicName).getCurrentBatchSize();
    }

    public long getCurrentBatchBytes(String topicName) {
        return getBatchContainer(topicName).getCurrentBatchBytes();
    }

    public void updateCurrentBatchSize(String topicName, long delta) {
        getBatchContainer(topicName).updateCurrentBatchSize(delta);
    }

    public void updateCurrentBatchBytes(String topicName, long delta) {
        getBatchContainer(topicName).updateCurrentBatchBytes(delta);
    }

    public boolean isEmpty() {
        return topicBatchContainer.values().stream().allMatch(BatchContainer::isEmpty);
    }

    public boolean needFlush() {
        return topicBatchContainer.values().stream().anyMatch(BatchContainer::needFlush);
    }

    public Map<String, List<Record<GenericRecord>>> poolFlushData() {
        Map<String, List<Record<GenericRecord>>> flushData = new HashMap<>();
        topicBatchContainer.forEach((topicName, batchContainer) -> {
            if (batchContainer.needFlush()) {
                flushData.put(topicName, batchContainer.poolNeedFlushRecords());
            }
        });
        return flushData;
    }

    private BatchContainer getBatchContainer(String topicName) {
        return topicBatchContainer.computeIfAbsent(topicName,
                k -> new BatchContainer(maxBatchSize, maxBatchBytes, maxPendingQueueSize));
    }
}
