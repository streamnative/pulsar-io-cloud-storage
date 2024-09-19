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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.format.Format;

/**
 * The LegacyPartitioner class, implementing the Partitioner interface, is designed to partition records according to
 * their respective topic names.
 * It is used to partition records based on their topic name. It will then use the legacy partitioner such as
 * SimplePartitioner or TimePartitioner as the underlying partitioner. This is for the backward compatibility usage.
 */
@Slf4j
public class LegacyPartitioner implements Partitioner {

    /**
     * This method partitions a list of records into a map where the keys are the topic names and the values are lists
     * of records.
     *
     * @param topicRecords A list of records of type GenericRecord that need to be partitioned.
     * @return A map where the keys are the topic names and the values are lists of records.
     */
    @Override
    public Map<String, List<Record<GenericRecord>>> partition(Map<String, List<Record<GenericRecord>>> topicRecords) {
        return topicRecords;
    }

    public String buildPartitionPath(Record<GenericRecord> message, String pathPrefix,
                                     org.apache.pulsar.io.jcloud.partitioner.legacy.Partitioner<GenericRecord> p,
                                     Format<?> format,
                                     long partitioningTimestamp) {

        String encodePartition = p.encodePartition(message, partitioningTimestamp);
        String partitionedPath = p.generatePartitionedPath(message.getTopicName().get(), encodePartition);
        String path = pathPrefix + partitionedPath + format.getExtension();
        log.info("generate message[recordSequence={}] savePath: {}", message.getRecordSequence().get(), path);
        return path;
    }
}
