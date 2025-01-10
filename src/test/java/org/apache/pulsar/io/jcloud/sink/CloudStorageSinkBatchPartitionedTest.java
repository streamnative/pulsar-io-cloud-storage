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
package org.apache.pulsar.io.jcloud.sink;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jcloud.format.Format;
import org.apache.pulsar.io.jcloud.writer.BlobWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

/**
 * Test for {@link CloudStorageGenericRecordSink}.
 */
public class CloudStorageSinkBatchPartitionedTest {

    private static final int PAYLOAD_BYTES = 100;

    @Mock
    private SinkContext mockSinkContext;

    @Mock
    private BlobWriter mockBlobWriter;

    @Mock
    private Record<GenericRecord> mockRecordTopic1;

    @Mock
    private Record<GenericRecord> mockRecordTopic2;

    private Map<String, Object> config;

    private CloudStorageGenericRecordSink sink;

    @Before
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setup() throws Exception {
        //initialize required parameters
        this.config = new HashMap<>();
        this.config.put("provider", "google-cloud-storage");
        this.config.put("bucket", "just/a/test");
        this.config.put("formatType", "bytes");
        this.config.put("partitionerType", "PARTITION");
        this.config.put("batchModel", "PARTITIONED");

        this.sink = spy(new CloudStorageGenericRecordSink());
        this.mockSinkContext = mock(SinkContext.class);
        this.mockBlobWriter = mock(BlobWriter.class);

        doReturn(mockBlobWriter).when(sink).initBlobWriter(any(CloudStorageSinkConfig.class));
        doReturn(ByteBuffer.wrap(new byte[]{0x0})).when(sink).bindValue(any(Iterator.class), any(Format.class));

        RecordSchemaBuilder schemaBuilder = SchemaBuilder.record("test");
        schemaBuilder.field("a").type(SchemaType.INT32).optional().defaultValue(null);
        GenericSchema<GenericRecord> schema = Schema.generic(schemaBuilder.build(SchemaType.JSON));
        GenericRecord genericRecord = spy(schema.newRecordBuilder().set("a", 1).build());
        doReturn(new byte[]{0x1}).when(genericRecord).getSchemaVersion();

        Message mockMessage = mock(Message.class);
        when(mockMessage.size()).thenReturn(PAYLOAD_BYTES);
        when(mockMessage.getMessageId()).thenReturn(new MessageIdImpl(12, 11, 1));

        Message mockMessage2 = mock(Message.class);
        when(mockMessage2.size()).thenReturn(PAYLOAD_BYTES);
        when(mockMessage2.getMessageId()).thenReturn(new MessageIdImpl(12, 34, 1));

        this.mockRecordTopic1 = mock(Record.class);
        when(mockRecordTopic1.getTopicName()).thenReturn(Optional.of("topic-1"));
        when(mockRecordTopic1.getValue()).thenReturn(genericRecord);
        when(mockRecordTopic1.getSchema()).thenAnswer((Answer<Schema>) invocationOnMock -> schema);
        when(mockRecordTopic1.getMessage()).thenReturn(Optional.of(mockMessage));

        this.mockRecordTopic2 = mock(Record.class);
        when(mockRecordTopic2.getTopicName()).thenReturn(Optional.of("topic-2"));
        when(mockRecordTopic2.getValue()).thenReturn(genericRecord);
        when(mockRecordTopic2.getSchema()).thenAnswer((Answer<Schema>) invocationOnMock -> schema);
        when(mockRecordTopic2.getMessage()).thenReturn(Optional.of(mockMessage2));
    }

    @After
    public void tearDown() throws Exception {
        this.sink.close();
    }

    @Test
    public void flushOnMaxBatchBytesTest() throws Exception {
        this.config.put("batchTimeMs", 60000); // set high batchTimeMs to prevent scheduled flush
        this.config.put("batchSize", 1000); // set high batchSize to prevent flush
        this.config.put("maxBatchBytes", 5 * PAYLOAD_BYTES); // force flush after 500 bytes
        verifySinkFlush();
    }

    @Test
    public void flushOnBatchSizeTests() throws Exception {
        this.config.put("batchTimeMs", 60000); // set high batchTimeMs to prevent scheduled flush
        this.config.put("maxBatchBytes", 10000); // set high maxBatchBytes to prevent flush
        this.config.put("batchSize", 5); // force flush after 5 messages

        verifySinkFlush();
    }

    @Test
    public void flushOnBatchSizeWithOutTopicNameTests() throws Exception {
        this.config.put("batchTimeMs", 60000); // set high batchTimeMs to prevent scheduled flush
        this.config.put("maxBatchBytes", 10000); // set high maxBatchBytes to prevent flush
        this.config.put("batchSize", 5); // force flush after 5 messages
        this.config.put("partitionerWithTopicName", "false");

        this.sink.open(this.config, this.mockSinkContext);

        for (int i = 0; i < 4; i++) {
            this.sink.write(mockRecordTopic1);
        }
        verify(mockBlobWriter, never()).uploadBlob(any(String.class), any(ByteBuffer.class));

        for (int i = 0; i < 5; i++) {
            this.sink.write(mockRecordTopic2);
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockBlobWriter, times(1))
                        .uploadBlob(eq("12.34.-1.raw"), any(ByteBuffer.class))
        );
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> verify(mockRecordTopic2, times(5)).ack()
        );

        this.sink.write(mockRecordTopic1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockBlobWriter, times(1))
                        .uploadBlob(eq("12.11.-1.raw"), any(ByteBuffer.class))
        );
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> verify(mockRecordTopic1, times(5)).ack()
        );
    }

    @Test
    public void flushOnBatchSizeWithOutTopicNamePathPrefixTests() throws Exception {
        this.config.put("batchTimeMs", 60000); // set high batchTimeMs to prevent scheduled flush
        this.config.put("maxBatchBytes", 10000); // set high maxBatchBytes to prevent flush
        this.config.put("batchSize", 5); // force flush after 5 messages
        this.config.put("partitionerWithTopicName", "false");
        this.config.put("pathPrefix", "testPrefix/");

        this.sink.open(this.config, this.mockSinkContext);

        for (int i = 0; i < 4; i++) {
            this.sink.write(mockRecordTopic1);
        }
        verify(mockBlobWriter, never()).uploadBlob(any(String.class), any(ByteBuffer.class));

        for (int i = 0; i < 5; i++) {
            this.sink.write(mockRecordTopic2);
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockBlobWriter, times(1))
                        .uploadBlob(eq("testPrefix/12.34.-1.raw"), any(ByteBuffer.class))
        );
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> verify(mockRecordTopic2, times(5)).ack()
        );

        this.sink.write(mockRecordTopic1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockBlobWriter, times(1))
                        .uploadBlob(eq("testPrefix/12.11.-1.raw"), any(ByteBuffer.class))
        );
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> verify(mockRecordTopic1, times(5)).ack()
        );
    }

    @Test
    public void flushOnTimeOutTests() throws Exception {
        long maxBatchTimeout = 2000;
        this.config.put("batchTimeMs", maxBatchTimeout); // set high batchTimeMs to prevent scheduled flush
        this.config.put("maxBatchBytes", 100000); // set high maxBatchBytes to prevent flush
        this.config.put("batchSize", 10); // force flush after 5 messages

        this.sink.open(this.config, this.mockSinkContext);

        // 0. Write 2 data for each topic
        for (int i = 0; i < 2; i++) {
            sink.write(mockRecordTopic1);
        }
        for (int i = 0; i < 2; i++) {
            sink.write(mockRecordTopic2);
        }

        // 1. First sleep maxBatchTimeout / 2, and not data need flush
        Thread.sleep(maxBatchTimeout / 2);
        verify(mockBlobWriter, never()).uploadBlob(any(String.class), any(ByteBuffer.class));

        // 2. Write 8 for topic-1 and to trigger flush
        for (int i = 0; i < 8; i++) {
            sink.write(mockRecordTopic1);
        }
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(10)).untilAsserted(
                () -> verify(mockBlobWriter, times(1))
                        .uploadBlob(eq("public/default/topic-1/12.11.-1.raw"), any(ByteBuffer.class))
        );
        verify(mockBlobWriter, never()).uploadBlob(eq("public/default/topic-2/12.34.-1.raw"), any(ByteBuffer.class));

        // 3. Write 2 message for topic-1 again and assert not message need flush(no timeout)
        for (int i = 0; i < 2; i++) {
            sink.write(mockRecordTopic1);
        }
        clearInvocations(mockBlobWriter);
        verify(mockBlobWriter, never()).uploadBlob(eq("public/default/topic-1/12.11.-1.raw"), any(ByteBuffer.class));

        // 4. Second sleep maxBatchTimeout / 2 again, and assert topic-2 data need flush
        //    and topic-1 no need flush(no timeout)
        Thread.sleep(maxBatchTimeout / 2 + 100);
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(10)).untilAsserted(
                () -> verify(mockBlobWriter, times(1))
                        .uploadBlob(eq("public/default/topic-2/12.34.-1.raw"), any(ByteBuffer.class))
        );
        verify(mockBlobWriter, never()).uploadBlob(eq("public/default/topic-1/12.11.-1.raw"), any(ByteBuffer.class));

        // 5. Assert for topic-1 flush data step-3 write data.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockBlobWriter, times(1))
                        .uploadBlob(eq("public/default/topic-1/12.11.-1.raw"), any(ByteBuffer.class))
        );

        // 6. Assert all message has been ack
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockRecordTopic1, times(12)).ack()
        );
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockRecordTopic2, times(2)).ack()
        );
    }

    @Test
    public void repeatedlyFlushOnBatchSizeTest() throws Exception {
        this.config.put("pendingQueueSize", 1000); // accept high number of messages
        this.config.put("batchTimeMs", 60000); // set high batchTimeMs to prevent scheduled flush
        this.config.put("maxBatchBytes", 100000); // set high maxBatchBytes to prevent flush
        this.config.put("batchSize", 5); // force flush after 5 messages

        verifyRecordAck(100, -1);
    }

    @Test
    public void repeatedlyFlushOnMaxBatchBytesTest() throws Exception {
        this.config.put("pendingQueueSize", 1000); // accept high number of messages
        this.config.put("batchTimeMs", 60000); // set high batchTimeMs to prevent scheduled flush
        this.config.put("maxBatchBytes", 5 * PAYLOAD_BYTES); // force flush after 500 bytes
        this.config.put("batchSize", 1000); // set high batchSize to prevent flush

        verifyRecordAck(100, -1);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void repeatedlyFlushOnMultiConditionTest() throws Exception {
        this.config.put("pendingQueueSize", 100);
        this.config.put("batchTimeMs", 1000);
        this.config.put("maxBatchBytes", 10 * PAYLOAD_BYTES);
        this.config.put("batchSize", 5);
        this.sink.open(this.config, this.mockSinkContext);

        // Gen random message size
        Message randomMessage = mock(Message.class);
        when(randomMessage.size()).thenAnswer((Answer<Integer>) invocation -> {
            int randomMultiplier = ThreadLocalRandom.current().nextInt(1, 6);
            return PAYLOAD_BYTES * randomMultiplier;
        });
        when(randomMessage.getMessageId()).thenReturn(new MessageIdImpl(12, 34, 1));
        when(mockRecordTopic1.getMessage()).thenReturn(Optional.of(randomMessage));
        when(mockRecordTopic2.getMessage()).thenReturn(Optional.of(randomMessage));

        int numberOfRecords = 100;
        for (int i = 0; i < numberOfRecords; i++) {
            this.sink.write(mockRecordTopic1);
            this.sink.write(mockRecordTopic2);
            Thread.sleep(ThreadLocalRandom.current().nextInt(1, 500));
        }
        await().atMost(Duration.ofSeconds(60)).untilAsserted(
                () -> verify(mockRecordTopic1, times(numberOfRecords)).ack()
        );
        await().atMost(Duration.ofSeconds(60)).untilAsserted(
                () -> verify(mockRecordTopic2, times(numberOfRecords)).ack()
        );
    }

    @Test
    public void testBatchCleanupWhenFlushCrashed() throws Exception {
        this.config.put("pendingQueueSize", 1000);
        this.config.put("batchTimeMs", 1000);
        this.config.put("maxBatchBytes", 5 * PAYLOAD_BYTES);
        this.config.put("batchSize", 1);

        this.sink.open(this.config, this.mockSinkContext);
        when(mockRecordTopic1.getSchema()).thenThrow(new OutOfMemoryError());
        when(mockRecordTopic2.getSchema()).thenThrow(new OutOfMemoryError());
        this.sink.write(mockRecordTopic1);
        this.sink.write(mockRecordTopic2);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> {
                    Assert.assertEquals(0, this.sink.batchManager.getCurrentBatchBytes("topic-1"));
                    Assert.assertEquals(0, this.sink.batchManager.getCurrentBatchSize("topic-1"));
                    Assert.assertEquals(0, this.sink.batchManager.getCurrentBatchBytes("topic-2"));
                    Assert.assertEquals(0, this.sink.batchManager.getCurrentBatchSize("topic-2"));
                }
        );
    }

    private void verifyRecordAck(int numberOfRecords, long sleepMillis) throws Exception {
        this.sink.open(this.config, this.mockSinkContext);
        for (int i = 0; i < numberOfRecords; i++) {
            this.sink.write(mockRecordTopic1);
            this.sink.write(mockRecordTopic2);
            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> verify(mockRecordTopic1, times(numberOfRecords)).ack()
        );
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> verify(mockRecordTopic2, times(numberOfRecords)).ack()
        );
    }

    private void verifySinkFlush() throws Exception {
        this.sink.open(this.config, this.mockSinkContext);

        for (int i = 0; i < 4; i++) {
            this.sink.write(mockRecordTopic1);
        }
        verify(mockBlobWriter, never()).uploadBlob(any(String.class), any(ByteBuffer.class));

        for (int i = 0; i < 5; i++) {
            this.sink.write(mockRecordTopic2);
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockBlobWriter, times(1))
                        .uploadBlob(eq("public/default/topic-2/12.34.-1.raw"), any(ByteBuffer.class))
        );
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> verify(mockRecordTopic2, times(5)).ack()
        );

        this.sink.write(mockRecordTopic1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockBlobWriter, times(1))
                        .uploadBlob(eq("public/default/topic-1/12.11.-1.raw"), any(ByteBuffer.class))
        );
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> verify(mockRecordTopic1, times(5)).ack()
        );
    }
}
