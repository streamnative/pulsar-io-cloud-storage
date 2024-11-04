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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.junit.Assert;
import org.junit.Test;

public class AbstractPartitionerTest {

    @Test
    public void testGetMessageOffsetWithBatchMessage() {

        AbstractPartitioner<Object> abstractPartitioner = new AbstractPartitioner<>() {
            @Override
            public String encodePartition(Record<Object> sinkRecord) {
                return null;
            }
        };

        BatchMessageIdImpl batchId1 = new BatchMessageIdImpl(12, 34, 1, 1);
        Record<Object> message1 = getMessageRecord(batchId1);

        BatchMessageIdImpl batchId2 = new BatchMessageIdImpl(12, 34, 1, 2);
        Record<Object> message2 = getMessageRecord(batchId2);

        Assert.assertNotEquals(abstractPartitioner.getMessageOffset(message1),
                abstractPartitioner.getMessageOffset(message2));

        MessageIdImpl id3 = new MessageIdImpl(12, 34, 1);
        Assert.assertEquals(abstractPartitioner.getMessageOffset(getMessageRecord(id3)), 3221225506L);
    }

    public static Record<Object> getMessageRecord(MessageId msgId) {
        @SuppressWarnings("unchecked")
        Message<Object> mock = mock(Message.class);
        when(mock.getPublishTime()).thenReturn(1599578218610L);
        when(mock.getMessageId()).thenReturn(msgId);
        when(mock.hasIndex()).thenReturn(true);
        when(mock.getIndex()).thenReturn(Optional.of(11115506L));

        String topic = TopicName.get("test").toString();
        Record<Object> mockRecord = mock(Record.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topic));
        when(mockRecord.getPartitionIndex()).thenReturn(Optional.of(1));
        when(mockRecord.getMessage()).thenReturn(Optional.of(mock));
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topic, 1)));
        when(mockRecord.getRecordSequence()).thenReturn(Optional.of(3221225506L));
        return mockRecord;
    }

}