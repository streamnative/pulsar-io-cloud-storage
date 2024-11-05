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
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
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
import org.apache.pulsar.io.jcloud.partitioner.Partitioner;
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
public class CloudStorageSinkBatchBlendTest {

    private static final int PAYLOAD_BYTES = 100;

    @Mock
    private SinkContext mockSinkContext;

    @Mock
    private BlobWriter mockBlobWriter;

    @Mock
    private Record<GenericRecord> mockRecord;

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
        this.config.put("partitionerType", "default");
        this.config.put("batchModel", "BLEND");

        this.sink = spy(new CloudStorageGenericRecordSink());
        this.mockSinkContext = mock(SinkContext.class);
        this.mockBlobWriter = mock(BlobWriter.class);
        this.mockRecord = mock(Record.class);

        doReturn("a/test.json").when(sink)
                .buildPartitionPath(any(Record.class), any(Partitioner.class), any(Format.class), any(Long.class));
        doReturn(mockBlobWriter).when(sink).initBlobWriter(any(CloudStorageSinkConfig.class));
        doReturn(ByteBuffer.wrap(new byte[]{0x0})).when(sink).bindValue(any(Iterator.class), any(Format.class));

        Message mockMessage = mock(Message.class);
        when(mockMessage.size()).thenReturn(PAYLOAD_BYTES);
        when(mockMessage.getMessageId()).thenReturn(new MessageIdImpl(12, 34, 1));

        GenericSchema<GenericRecord> schema = createTestSchema();
        GenericRecord genericRecord = spy(createTestRecord(schema));
        doReturn(new byte[]{0x1}).when(genericRecord).getSchemaVersion();

        when(mockRecord.getTopicName()).thenReturn(Optional.of("test-topic"));
        when(mockRecord.getValue()).thenReturn(genericRecord);
        when(mockRecord.getSchema()).thenAnswer((Answer<Schema>) invocationOnMock -> schema);
        when(mockRecord.getMessage()).thenReturn(Optional.of(mockMessage));
    }

    @After
    public void tearDown() throws Exception {
        this.sink.close();
    }

    private static GenericRecord createTestRecord(GenericSchema<GenericRecord> schema) {
        return schema.newRecordBuilder().set("a", 1).build();
    }

    private static GenericSchema<GenericRecord> createTestSchema() {
        RecordSchemaBuilder schemaBuilder = SchemaBuilder.record("test");
        schemaBuilder.field("a").type(SchemaType.INT32).optional().defaultValue(null);
        return Schema.generic(schemaBuilder.build(SchemaType.JSON));
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
    public void repeatedlyFlushOnBatchSizeTest() throws Exception {
        this.config.put("pendingQueueSize", 1000); // accept high number of messages
        this.config.put("batchTimeMs", 60000); // set high batchTimeMs to prevent scheduled flush
        this.config.put("maxBatchBytes", 100000); // set high maxBatchBytes to prevent flush
        this.config.put("batchSize", 5); // force flush after 5 messages

        verifyRecordAck(100);
    }
    @Test
    public void repeatedlyFlushOnMaxBatchBytesTest() throws Exception {
        this.config.put("pendingQueueSize", 1000); // accept high number of messages
        this.config.put("batchTimeMs", 60000); // set high batchTimeMs to prevent scheduled flush
        this.config.put("maxBatchBytes", 5 * PAYLOAD_BYTES); // force flush after 500 bytes
        this.config.put("batchSize", 1000); // set high batchSize to prevent flush

        verifyRecordAck(100);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void repeatedlyFlushOnMultiConditionTest() throws Exception {
        this.config.put("pendingQueueSize", 100); // accept high number of messages
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
        when(mockRecord.getMessage()).thenReturn(Optional.of(randomMessage));

        int numberOfRecords = 100;
        for (int i = 0; i < numberOfRecords; i++) {
            this.sink.write(mockRecord);
            Thread.sleep(ThreadLocalRandom.current().nextInt(1, 500));
        }
        await().atMost(Duration.ofSeconds(60)).untilAsserted(
                () -> verify(mockRecord, times(numberOfRecords)).ack()
        );
    }

    @Test
    public void testBatchCleanupWhenFlushCrashed() throws Exception {
        this.config.put("pendingQueueSize", 1000);
        this.config.put("batchTimeMs", 1000);
        this.config.put("maxBatchBytes", 5 * PAYLOAD_BYTES);
        this.config.put("batchSize", 1);

        this.sink.open(this.config, this.mockSinkContext);
        when(mockRecord.getSchema()).thenThrow(new OutOfMemoryError());
        sendMockRecord(1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> {
                    Assert.assertEquals(0, this.sink.batchManager.getCurrentBatchBytes("test-topic"));
                    Assert.assertEquals(0, this.sink.batchManager.getCurrentBatchSize("test-topic"));
                }
        );
    }

    private void verifyRecordAck(int numberOfRecords) throws Exception {
        this.sink.open(this.config, this.mockSinkContext);
        sendMockRecord(numberOfRecords);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> verify(mockRecord, times(numberOfRecords)).ack()
        );
    }

    private void verifySinkFlush() throws Exception {
        this.sink.open(this.config, this.mockSinkContext);

        sendMockRecord(4);
        verify(mockBlobWriter, never()).uploadBlob(any(String.class), any(ByteBuffer.class));

        sendMockRecord(3);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockBlobWriter, atLeastOnce()).uploadBlob(any(String.class), any(ByteBuffer.class))
        );
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> verify(mockRecord, atLeast(5)).ack()
        );
    }

    private void sendMockRecord(int numberOfRecords) throws Exception {
        for (int i = 0; i < numberOfRecords; i++) {
            this.sink.write(mockRecord);
        }
    }


}
