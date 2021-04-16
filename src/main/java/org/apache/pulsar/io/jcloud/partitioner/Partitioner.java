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
package org.apache.pulsar.io.jcloud.partitioner;

import java.io.File;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;

/**
 * Partition incoming records, and generates directories and file names in which to store the
 * incoming records.
 *
 * @param <T> The type representing the field schemas.
 */
public interface Partitioner<T> {

    String PATH_SEPARATOR = File.separator;


    void configure(BlobStoreAbstractConfig config);

    /**
     * Returns string representing the output path for a sinkRecord to be encoded and stored.
     *
     * @param sinkRecord The record to be stored by the Sink Connector
     * @return The path/filename the SinkRecord will be stored into after it is encoded
     */
    String encodePartition(Record<T> sinkRecord);

    /**
     * Returns string representing the output path for a sinkRecord to be encoded and stored.
     *
     * @param sinkRecord  The record to be stored by the Sink Connector
     * @param nowInMillis The current time in ms. Some Partitioners will use this option, but by
     *                    default it is unused.
     * @return The path/filename the SinkRecord will be stored into after it is encoded
     */
    default String encodePartition(Record<T> sinkRecord, long nowInMillis) {
        return encodePartition(sinkRecord);
    }

    default String generatePartitionedPath(String topic, String encodedPartition) {
        TopicName topicName = TopicName.get(topic);
        return StringUtils.joinWith(PATH_SEPARATOR,
                topicName.getTenant(),
                topicName.getNamespacePortion(),
                topicName.getLocalName(),
                encodedPartition
        );
    }
}
