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
package org.apache.pulsar.io.jcloud.partitioner.legacy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simplify the common parts of the build path.
 *
 * @param <T> The type representing the field schemas.
 */
public abstract class AbstractPartitioner<T> implements Partitioner<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPartitioner.class);

    private boolean sliceTopicPartitionPath;
    private boolean withTopicPartitionNumber;
    private boolean useIndexAsOffset;

    @Override
    public void configure(BlobStoreAbstractConfig config) {
        this.sliceTopicPartitionPath = config.isSliceTopicPartitionPath();
        this.withTopicPartitionNumber = config.isWithTopicPartitionNumber();
        this.useIndexAsOffset = config.isPartitionerUseIndexAsOffset();
    }

    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {

        List<String> joinList = new ArrayList<>();
        TopicName topicName = TopicName.get(topic);
        joinList.add(topicName.getTenant());
        joinList.add(topicName.getNamespacePortion());

        if (topicName.isPartitioned() && withTopicPartitionNumber) {
            if (sliceTopicPartitionPath) {
                TopicName newTopicName = TopicName.get(topicName.getPartitionedTopicName());
                joinList.add(newTopicName.getLocalName());
                joinList.add(Integer.toString(topicName.getPartitionIndex()));
            } else {
                joinList.add(topicName.getLocalName());
            }
        } else {
            TopicName newTopicName = TopicName.get(topicName.getPartitionedTopicName());
            joinList.add(newTopicName.getLocalName());
        }
        joinList.add(encodedPartition);
        return StringUtils.join(joinList, PATH_SEPARATOR);
    }

    protected long getMessageOffset(Record<T> record) {
        if (useIndexAsOffset && record.getMessage().isPresent()) {
            final Message<T> message = record.getMessage().get();
            // Use index added by org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor if present.
            // Requires exposingBrokerEntryMetadataToClientEnabled=true on brokers.
            if (message.hasIndex()) {
                final Optional<Long> index = message.getIndex();
                if (index.isPresent()) {
                    return index.get();
                } else {
                    LOGGER.warn("Found message {} with hasIndex=true but index is empty, using recordSequence",
                            message.getMessageId());
                }
            } else {
                LOGGER.warn("partitionerUseIndexAsOffset configured to true but no index found on the message {}, "
                                + "perhaps the broker didn't exposed the metadata, using recordSequence",
                        message.getMessageId());
            }
        }
        return record.getRecordSequence()
                .orElseThrow(() -> new RuntimeException("found empty recordSequence"));
    }
}
