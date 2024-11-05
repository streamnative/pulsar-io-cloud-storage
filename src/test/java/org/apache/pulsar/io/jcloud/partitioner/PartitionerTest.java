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
import com.google.common.base.Supplier;
import java.text.MessageFormat;
import java.util.Optional;
import junit.framework.TestCase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


/**
 * partitioner unit test.
 */
@RunWith(Parameterized.class)
public class PartitionerTest extends TestCase {

    @Parameterized.Parameter(0)
    public Partitioner<Object> partitioner;

    @Parameterized.Parameter(1)
    public String expected;

    @Parameterized.Parameter(2)
    public String expectedPartitionedPath;

    @Parameterized.Parameter(3)
    public Record<Object> pulsarRecord;

    private static final MessageId testMessageId = new MessageIdImpl(12, 34, 1);
    private static final String testMsgIdFileName = "12.34.-1";

    @Parameterized.Parameters
    public static Object[][] data() {
        BlobStoreAbstractConfig blobStoreAbstractConfig = new BlobStoreAbstractConfig();
        blobStoreAbstractConfig.setTimePartitionDuration("1d");
        blobStoreAbstractConfig.setTimePartitionPattern("yyyy-MM-dd");
        SimplePartitioner<Object> simplePartitioner = new SimplePartitioner<>();
        simplePartitioner.configure(blobStoreAbstractConfig);
        TimePartitioner<Object> dayPartitioner = new TimePartitioner<>();
        dayPartitioner.configure(blobStoreAbstractConfig);

        SimplePartitioner<Object> simplePartitionerIndexOffset = new SimplePartitioner<>();
        blobStoreAbstractConfig = new BlobStoreAbstractConfig();
        blobStoreAbstractConfig.setPartitionerUseIndexAsOffset(true);
        simplePartitionerIndexOffset.configure(blobStoreAbstractConfig);

        BlobStoreAbstractConfig hourConfig = new BlobStoreAbstractConfig();
        hourConfig.setTimePartitionDuration("4h");
        hourConfig.setTimePartitionPattern("yyyy-MM-dd-HH");
        TimePartitioner<Object> hourPartitioner = new TimePartitioner<>();
        hourPartitioner.configure(hourConfig);

        BlobStoreAbstractConfig noPartitionNumberblobStoreAbstractConfig = new BlobStoreAbstractConfig();
        noPartitionNumberblobStoreAbstractConfig.setTimePartitionDuration("1d");
        noPartitionNumberblobStoreAbstractConfig.setTimePartitionPattern("yyyy-MM-dd");
        noPartitionNumberblobStoreAbstractConfig.setWithTopicPartitionNumber(false);
        SimplePartitioner<Object> noPartitionNumberPartitioner = new SimplePartitioner<>();
        noPartitionNumberPartitioner.configure(noPartitionNumberblobStoreAbstractConfig);

        BlobStoreAbstractConfig numberConfig = new BlobStoreAbstractConfig();
        numberConfig.setTimePartitionDuration("7200000");
        numberConfig.setTimePartitionPattern("yyyy-MM-dd-HH");
        TimePartitioner<Object> numberPartitioner = new TimePartitioner<>();
        numberPartitioner.configure(numberConfig);
        return new Object[][]{
                new Object[]{
                        simplePartitioner,
                        testMsgIdFileName,
                        "public/default/test" + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        getTopic()
                },
                new Object[]{
                        simplePartitionerIndexOffset,
                        "11115506",
                        "public/default/test" + Partitioner.PATH_SEPARATOR + "11115506",
                        getTopic()
                },
                new Object[]{
                        dayPartitioner,
                        "2020-09-08" + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        "public/default/test/2020-09-08" + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        getTopic()
                },
                new Object[]{
                        hourPartitioner,
                        "2020-09-08-12" + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        "public/default/test/2020-09-08-12" + Partitioner.PATH_SEPARATOR + testMsgIdFileName
                        , getTopic()
                },
                new Object[]{
                        simplePartitioner,
                        testMsgIdFileName,
                        "public/default/test-partition-1" + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        getPartitionedTopic()
                },
                new Object[]{
                        dayPartitioner,
                        "2020-09-08" + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        "public/default/test-partition-1/2020-09-08" + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        getPartitionedTopic()
                },
                new Object[]{
                        hourPartitioner,
                        "2020-09-08-12" + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        "public/default/test-partition-1/2020-09-08-12" + Partitioner.PATH_SEPARATOR + testMsgIdFileName
                        , getPartitionedTopic()
                },
                new Object[]{
                        noPartitionNumberPartitioner,
                        testMsgIdFileName,
                        "public/default/test" + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        getPartitionedTopic()
                },
                new Object[]{
                        numberPartitioner,
                        "2020-09-08-14" + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        "public/default/test-partition-1/2020-09-08-14"
                                + Partitioner.PATH_SEPARATOR + testMsgIdFileName,
                        getPartitionedTopic()
                },
        };
    }

    public static Record<Object> getPartitionedTopic() {
        @SuppressWarnings("unchecked")
        Message<Object> mock = mock(Message.class);
        when(mock.getPublishTime()).thenReturn(1599578218610L);
        when(mock.getMessageId()).thenReturn(testMessageId);
        String topic = TopicName.get("test-partition-1").toString();
        Record<Object> mockRecord = mock(Record.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topic));
        when(mockRecord.getPartitionIndex()).thenReturn(Optional.of(1));
        when(mockRecord.getMessage()).thenReturn(Optional.of(mock));
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topic, 1)));
        return mockRecord;
    }

    public static Record<Object> getTopic() {
        @SuppressWarnings("unchecked")
        Message<Object> mock = mock(Message.class);
        when(mock.getPublishTime()).thenReturn(1599578218610L);
        when(mock.getMessageId()).thenReturn(testMessageId);
        when(mock.hasIndex()).thenReturn(true);
        when(mock.getIndex()).thenReturn(Optional.of(11115506L));

        String topic = TopicName.get("test").toString();
        Record<Object> mockRecord = mock(Record.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topic));
        when(mockRecord.getPartitionIndex()).thenReturn(Optional.of(1));
        when(mockRecord.getMessage()).thenReturn(Optional.of(mock));
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topic, 1)));
        return mockRecord;
    }

    @Test
    public void testEncodePartition() {
        String encodePartition = partitioner.encodePartition(pulsarRecord, System.currentTimeMillis());
        Supplier<String> supplier =
                () -> MessageFormat.format("expected: {0}\nactual: {1}", expected, encodePartition);
        Assert.assertEquals(supplier.get(), expected, encodePartition);
    }

    @Test
    public void testGeneratePartitionedPath() {
        String encodePartition = partitioner.encodePartition(pulsarRecord, System.currentTimeMillis());
        String partitionedPath =
                partitioner.generatePartitionedPath(pulsarRecord.getTopicName().get(), encodePartition);

        Supplier<String> supplier =
                () -> MessageFormat.format("expected: {0}\nactual: {1}", expected, encodePartition);
        Assert.assertEquals(supplier.get(), expectedPartitionedPath, partitionedPath);
    }
}