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
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;

/**
 * The BatchManager interface defines the operations that a batch manager should provide.
 * A batch manager handles the batching of records for more efficient processing.
 */
public interface BatchManager {

    /**
     * Creates a BatchManager based on the provided configuration.
     * @param config the configuration to use when creating the BatchManager
     * @return a BatchManager instance
     * @throws IllegalArgumentException if the batch model specified in the configuration is unsupported
     */
    static BatchManager createBatchManager(BlobStoreAbstractConfig config) {
        switch (config.getBatchModel()) {
            case BLEND:
                return new BlendBatchManager(config.getBatchSize(),
                        config.getMaxBatchBytes(), config.getPendingQueueSize());
            case PARTITIONED:
                return new PartitionedBatchManager(config.getBatchSize(),
                        config.getMaxBatchBytes(), config.getPendingQueueSize());
            default:
                throw new IllegalArgumentException("Unsupported batch model: " + config.getBatchModel());
        }
    }

    /**
     * Calculate the sum of the byte sizes of the messages in a list of records.
     *
     * @param records The list of records whose message sizes are to be summed.
     * @return The sum of the byte sizes of the messages in the given records.
     */
    static long getBytesSum(List<Record<GenericRecord>> records) {
        return records.stream()
                .map(Record::getMessage)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .mapToLong(Message::size)
                .sum();
    }

    /**
     * Adds a record to the batch manager.
     * @param record the record to add
     * @throws InterruptedException if the adding process is interrupted
     */
    void add(Record<GenericRecord> record) throws InterruptedException;

    /**
     * Determines whether the current batch needs to be flushed.
     * @return true if the batch needs to be flushed, false otherwise
     */
    boolean needFlush();

    /**
     * Retrieves the data that needs to be flushed.
     * @return a map where the keys are the topic names and the values are the lists of records for each topic
     */
    Map<String, List<Record<GenericRecord>>> poolFlushData();

    /**
     * Retrieves the current batch size for a given topic.
     * @param topicName the name of the topic
     * @return the current batch size
     */
    long getCurrentBatchSize(String topicName);

    /**
     * Retrieves the current batch bytes for a given topic.
     * @param topicName the name of the topic
     * @return the current batch bytes
     */
    long getCurrentBatchBytes(String topicName);

    /**
     * Updates the current batch size for a given topic.
     * @param topicName the name of the topic
     * @param delta the amount to add to the current batch size
     */
    void updateCurrentBatchSize(String topicName, long delta);

    /**
     * Updates the current batch bytes for a given topic.
     * @param topicName the name of the topic
     * @param delta the amount to add to the current batch bytes
     */
    void updateCurrentBatchBytes(String topicName, long delta);

    /**
     * Determines whether the batch manager is currently empty.
     * @return true if the batch manager is empty, false otherwise
     */
    boolean isEmpty();
}
