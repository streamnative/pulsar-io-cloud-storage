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
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * BlendBatchManager is a type of BatchManager that uses a single BatchContainer
 * for all topics. This means that all records, regardless of topic, are batched together.
 */
public class BlendBatchManager implements BatchManager {

    private final BatchContainer batchContainer;

    public BlendBatchManager(long maxBatchSize, long maxBatchBytes, int maxPendingQueueSize) {
        batchContainer = new BatchContainer(maxBatchSize, maxBatchBytes, maxPendingQueueSize);
    }

    public void add(Record<GenericRecord> record) throws InterruptedException {
        batchContainer.add(record);
    }

    public long getCurrentBatchSize(String topicName) {
        return batchContainer.getCurrentBatchSize();
    }

    public long getCurrentBatchBytes(String topicName) {
        return batchContainer.getCurrentBatchBytes();
    }

    public void updateCurrentBatchSize(String topicName, long delta) {
        batchContainer.updateCurrentBatchSize(delta);
    }

    public void updateCurrentBatchBytes(String topicName, long delta) {
        batchContainer.updateCurrentBatchBytes(delta);
    }

    public boolean isEmpty() {
        return batchContainer.isEmpty();
    }

    public boolean needFlush() {
        return batchContainer.needFlush();
    }

    public Map<String, List<Record<GenericRecord>>> poolFlushData() {
        List<Record<GenericRecord>> records = batchContainer.poolNeedFlushRecords();
        return records.stream().collect(Collectors.groupingBy(record -> record.getTopicName().get()));
    }
}
