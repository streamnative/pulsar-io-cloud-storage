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
package org.apache.pulsar.ecosystem.io.s3.partitioner;

import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.ecosystem.io.s3.BlobStoreAbstractConfig;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * use topic partition strategy.
 * @param <T> config
 */
public class SimplePartitioner<T> implements Partitioner<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimplePartitioner.class);

    @Override
    public void configure(BlobStoreAbstractConfig config) {

    }

    @Override
    public String encodePartition(Record<T> sinkRecord) {
        Optional<Long> recordSequence = sinkRecord.getRecordSequence();
        return recordSequence.map(String::valueOf).orElseThrow(() -> new RuntimeException("recordSequence not null"));
    }

    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
        TopicName topicName = TopicName.get(topic);
        return StringUtils.joinWith("/",
                topicName.getTenant(),
                topicName.getNamespacePortion(),
                topicName.getLocalName(),
                encodedPartition
        );
    }

}
