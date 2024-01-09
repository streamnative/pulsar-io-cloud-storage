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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;

/**
 * The TopicPartitioner is used to partition records based on the topic name.
 */
public class TopicPartitioner implements Partitioner {
    @Override
    public Map<String, List<Record<GenericRecord>>> partition(List<Record<GenericRecord>> records) {
        return records.stream()
                .collect(Collectors.groupingBy(record -> record.getTopicName()
                        .orElseThrow(() -> new RuntimeException("Topic name is not present in record."))))
                .entrySet().stream()
                .collect(Collectors.toMap(entry -> generateFilePath(entry.getKey(), records), Map.Entry::getValue));
    }

    String generateFilePath(String topic, List<Record<GenericRecord>> records) {
        TopicName topicName = TopicName.getPartitionedTopicName(topic);

        return StringUtils.join(Arrays.asList(
                topicName.getTenant(),
                topicName.getNamespacePortion(),
                topicName.getLocalName(),
                Long.toString(getMessageOffset(records.get(0)))
        ), File.separator);
    }

    protected long getMessageOffset(Record<GenericRecord> record) {
        return record.getRecordSequence()
                .orElseThrow(() -> new RuntimeException("The record sequence is not present in record."));
    }
}
