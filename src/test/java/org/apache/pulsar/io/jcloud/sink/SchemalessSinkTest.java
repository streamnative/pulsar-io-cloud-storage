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

import static org.apache.pulsar.io.jcloud.schema.TopicSchemaFallbackTest.TOPIC;
import static org.apache.pulsar.io.jcloud.schema.TopicSchemaFallbackTest.bytes;
import static org.apache.pulsar.io.jcloud.schema.TopicSchemaFallbackTest.context;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jcloud.format.ParquetInputFile;
import org.apache.pulsar.io.jcloud.writer.BlobWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SchemalessSinkTest {
    private CloudStorageGenericRecordSink sink;
    private SinkContext context;
    private BlobWriter writer;
    private Map<String, Object> config;
    private SchemaInfo info;

    @Before
    public void setup() throws Exception {
        info = SchemaInfo.builder().type(SchemaType.JSON).name("Event")
                .schema(SchemaBuilder.record("Event").fields().requiredInt("id").endRecord()
                        .toString().getBytes(StandardCharsets.UTF_8)).properties(Map.of()).build();
        context = context(info, 0);
        writer = mock(BlobWriter.class);
        sink = spy(new CloudStorageGenericRecordSink());
        doReturn(writer).when(sink).initBlobWriter(any());
        doReturn("test.parquet").when(sink).buildPartitionPath(any(), any(), any(), any(Long.class));
        config = new HashMap<>(Map.of("provider", "google-cloud-storage", "bucket", "test-bucket",
                "formatType", "parquet", "partitionerType", "partition", "batchSize", 2,
                "batchTimeMs", 60000, "parquetCodec", "none", "topicSchemaFallback",
                Map.of("enabled", true, "topicVersions", Map.of(TOPIC, 0))));
    }

    @After
    public void cleanup() throws Exception {
        sink.close();
    }

    private Record<GenericRecord> record(String json) {
        return bytes(TOPIC, json.getBytes(StandardCharsets.UTF_8));
    }

    private void awaitAck(Record<GenericRecord> record) {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(record).ack());
    }

    @Test
    public void goodBadGoodProducesParquetAndOnlyBadRedelivers() throws Exception {
        sink.open(config, context);
        Record<GenericRecord> first = record("{\"id\":1}");
        Record<GenericRecord> bad = record("{\"id\":\"secret-value\"}");
        Record<GenericRecord> last = record("{\"id\":3}");
        sink.write(first);
        sink.write(bad);
        sink.write(last);
        awaitAck(last);
        verify(first).ack();
        verify(bad).fail();
        verify(bad, never()).ack();
        verify(context, never()).fatal(any());
        verify(context).recordMetric("_cloud_storage_schemaless_malformed_", 1);
        verify(context).recordMetric("_cloud_storage_schemaless_redelivered_", 1);
        ArgumentCaptor<ByteBuffer> payload = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(writer).uploadBlob(anyString(), payload.capture());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(payload.getValue().array());
        try (ParquetReader<org.apache.avro.generic.GenericRecord> reader = AvroParquetReader
                .<org.apache.avro.generic.GenericRecord>builder(new ParquetInputFile("sink.parquet", output))
                .withDataModel(GenericData.get()).build()) {
            assertEquals(1, reader.read().get("id"));
            assertEquals(3, reader.read().get("id"));
            assertNull(reader.read());
        }
    }

    @Test
    public void dropRequiresExplicitChoice() throws Exception {
        config.put("schemalessDecodeFailureAction", "LOG_AND_DROP");
        sink.open(config, context);
        Record<GenericRecord> bad = record("null");
        sink.write(bad);
        verify(bad).ack();
        verify(bad, never()).fail();
        verify(context).recordMetric("_cloud_storage_schemaless_explicitly_dropped_", 1);
        verify(writer, never()).uploadBlob(anyString(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void disabledModeKeepsBytesRejection() throws Exception {
        config.remove("topicSchemaFallback");
        sink.open(config, context);
        Record<GenericRecord> first = record("{\"id\":1}");
        Record<GenericRecord> last = record("{\"id\":2}");
        when(first.getSchema()).thenReturn((Schema) Schema.BYTES);
        when(last.getSchema()).thenReturn((Schema) Schema.BYTES);
        sink.write(first);
        sink.write(last);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(last).fail());
        verify(first).fail();
        verify(first, never()).ack();
        verify(writer, never()).uploadBlob(anyString(), any());
        verify((PulsarClientImpl) context.getPulsarClient(), never()).getSchema(anyString());
    }

    @Test
    public void typedAndFallbackUseSeparateWriters() throws Exception {
        sink.open(config, context);
        Record<GenericRecord> typed = record("unused");
        Schema<GenericRecord> schema = Schema.generic(info);
        when(typed.getSchema()).thenReturn(schema);
        when(typed.getValue()).thenReturn(schema.decode("{\"id\":2}".getBytes(StandardCharsets.UTF_8)));
        Record<GenericRecord> fallback = record("{\"id\":1}");
        sink.write(typed);
        sink.write(fallback);
        awaitAck(fallback);
        verify(typed).ack();
        verify(writer, times(2)).uploadBlob(anyString(), any());
    }

    @Test
    public void sameVersionDifferentTopicsDoNotShareCacheOrWriter() throws Exception {
        String other = TOPIC + "-other";
        config.put("topicSchemaFallback", Map.of("enabled", true, "topicVersions", Map.of(TOPIC, 0, other, 0)));
        config.put("batchSize", 3);
        LookupService lookup = ((PulsarClientImpl) context.getPulsarClient()).getLookup(TOPIC);
        SchemaInfo otherInfo = SchemaInfo.builder().type(SchemaType.JSON).name("Other")
                .schema(SchemaBuilder.record("Other").fields().requiredString("id").endRecord()
                        .toString().getBytes(StandardCharsets.UTF_8)).properties(Map.of()).build();
        when(lookup.getSchema(eq(TopicName.get(other)), any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(java.util.Optional.of(otherInfo)));
        sink.open(config, context);
        Record<GenericRecord> one = bytes(TOPIC + "-partition-0", "{\"id\":1}".getBytes(StandardCharsets.UTF_8));
        Record<GenericRecord> two = bytes(TOPIC + "-partition-1", "{\"id\":2}".getBytes(StandardCharsets.UTF_8));
        Record<GenericRecord> three = bytes(other, "{\"id\":\"three\"}".getBytes(StandardCharsets.UTF_8));
        sink.write(one);
        sink.write(two);
        sink.write(three);
        awaitAck(three);
        awaitAck(one);
        awaitAck(two);
        verify(writer, times(3)).uploadBlob(anyString(), any());
        verify(lookup, times(2)).getSchema(any(TopicName.class), any(byte[].class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void unsupportedFirstGroupDoesNotStrandSecondGroup() throws Exception {
        sink.open(config, context);
        Record<GenericRecord> unsupported = record("{\"id\":1}");
        when(unsupported.getMessage().get().getSchemaVersion()).thenReturn(new byte[]{1});
        when(unsupported.getSchema()).thenReturn((Schema) Schema.BYTES);
        Record<GenericRecord> good = record("{\"id\":2}");
        sink.write(unsupported);
        sink.write(good);
        awaitAck(good);
        verify(unsupported, never()).ack();
        verify(unsupported, never()).fail();
        verify(unsupported.getMessage().get()).release();
        verify(context).fatal(any());
    }

    @Test
    public void uploadFailureNeverUsesDropOrPoisonMessageRetries() throws Exception {
        config.put("schemalessDecodeFailureAction", "LOG_AND_DROP");
        doThrow(new IOException("storage unavailable")).when(writer).uploadBlob(anyString(), any());
        sink.open(config, context);
        Record<GenericRecord> first = record("{\"id\":1}");
        Record<GenericRecord> last = record("{\"id\":2}");
        sink.write(first);
        sink.write(last);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(context).pause(TOPIC, -1));
        for (Record<GenericRecord> record : List.of(first, last)) {
            verify(record, never()).ack();
            verify(record, never()).fail();
            verify(record.getMessage().get()).release();
        }
        verify(writer, times(3)).uploadBlob(anyString(), any());
        verify(context, times(3)).recordMetric("_cloud_storage_upload_failure_", 1);
        verify(context).recordMetric("_cloud_storage_sink_paused_", 1);
        verify(context, never()).fatal(any());
        verify(context).pause(TOPIC, -1);
    }

    @Test
    public void schemaLookupFailureInFirstGroupDoesNotStrandValidRecords() throws Exception {
        sink.open(config, context);
        Record<GenericRecord> broken = record("{\"id\":1}");
        when(broken.getMessage().get().getSchemaVersion()).thenReturn(new byte[]{1});
        when(broken.getSchema()).thenThrow(new IllegalStateException("schema unavailable"));
        Record<GenericRecord> good = record("{\"id\":2}");
        sink.write(broken);
        sink.write(good);
        awaitAck(good);
        verify(broken.getMessage().get()).release();
        verify(broken, never()).ack();
        verify(broken, never()).fail();
    }

    @Test
    public void writerBugIsNeverARecordLevelDrop() throws Exception {
        config.put("schemalessDecodeFailureAction", "LOG_AND_DROP");
        doThrow(new IllegalStateException("writer bug")).when(sink).bindValue(any(), any());
        sink.open(config, context);
        Record<GenericRecord> first = record("{\"id\":1}");
        Record<GenericRecord> last = record("{\"id\":2}");
        sink.write(first);
        sink.write(last);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(context).fatal(any()));
        for (Record<GenericRecord> record : List.of(first, last)) {
            verify(record, never()).ack();
            verify(record, never()).fail();
            verify(record.getMessage().get()).release();
        }
        verify(writer, never()).uploadBlob(anyString(), any());
        verify(context, never()).recordMetric("_cloud_storage_schemaless_explicitly_dropped_", 1);
    }

    @Test
    public void terminationAfterUploadBeforeAckLeavesBothRecordsUnacknowledged() throws Exception {
        doAnswer(call -> {
            throw new AssertionError("simulate process termination after persisting object");
        }).when(writer).uploadBlob(anyString(), any());
        sink.open(config, context);
        Record<GenericRecord> first = record("{\"id\":1}");
        Record<GenericRecord> last = record("{\"id\":2}");
        sink.write(first);
        sink.write(last);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(context).fatal(any()));
        for (Record<GenericRecord> record : List.of(first, last)) {
            verify(record, never()).ack();
            verify(record, never()).fail();
            verify(record.getMessage().get()).release();
        }
        verify(writer).uploadBlob(anyString(), any());
    }

    @Test
    public void postUploadMetricsFailureCannotFailAcknowledgedRecords() throws Exception {
        doThrow(new IllegalStateException("metrics failed")).when(context).recordMetric(anyString(), anyDouble());
        sink.open(config, context);
        Record<GenericRecord> first = record("{\"id\":1}");
        Record<GenericRecord> last = record("{\"id\":2}");
        sink.write(first);
        sink.write(last);
        awaitAck(last);
        verify(first).ack();
        verify(first, never()).fail();
        verify(last, never()).fail();
        verify(context, never()).fatal(any());
        verify(writer).uploadBlob(anyString(), any());
    }

    @Test
    public void throwingAckDoesNotRepeatCompletionOrRelease() throws Exception {
        sink.open(config, context);
        Record<GenericRecord> first = record("{\"id\":1}");
        Record<GenericRecord> last = record("{\"id\":2}");
        doAnswer(call -> {
            first.getMessage().get().release();
            throw new IllegalStateException("ack callback failed after release");
        }).when(first).ack();
        sink.write(first);
        sink.write(last);
        awaitAck(last);
        verify(first).ack();
        verify(first, never()).fail();
        verify(first.getMessage().get()).release();
        verify(last, never()).fail();
    }

    @Test
    public void shutdownReleasesPendingWithoutAckOrNegativeAck() throws Exception {
        sink.open(config, context);
        Record<GenericRecord> pending = record("{\"id\":1}");
        sink.write(pending);
        sink.close();
        verify(pending, never()).ack();
        verify(pending, never()).fail();
        verify(pending.getMessage().get()).release();
        Record<GenericRecord> afterClose = record("{\"id\":2}");
        when(afterClose.getValue()).thenThrow(new AssertionError("Stopped input must not decode the record"));
        sink.write(afterClose);
        verify(afterClose.getMessage().get()).release();
        verify(afterClose, never()).fail();
    }

    @Test
    public void uploadRetryUsesSamePathAndFreshBuffer() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        doAnswer(call -> {
            ByteBuffer buffer = call.getArgument(1);
            assertEquals(0, buffer.position());
            buffer.position(buffer.limit());
            if (attempts.incrementAndGet() == 1) {
                throw new IOException("temporary outage");
            }
            return null;
        }).when(writer).uploadBlob(eq("test.parquet"), any());
        sink.open(config, context);
        Record<GenericRecord> first = record("{\"id\":1}");
        Record<GenericRecord> last = record("{\"id\":2}");
        sink.write(first);
        sink.write(last);
        awaitAck(last);
        assertEquals(2, attempts.get());
        verify(first).ack();
        verify(first, never()).fail();
    }

    @Test
    public void replaysCannotOverwritePreviousBatchObjects() throws Exception {
        doCallRealMethod().when(sink).buildPartitionPath(any(), any(), any(), any(Long.class));
        sink.open(config, context);
        Record<GenericRecord> record = record("{\"id\":1}");
        when(record.getMessage().get().getMessageId()).thenReturn(new MessageIdImpl(12, 34, -1));
        String first = sink.buildPartitionPath(record, sink.partitioner, sink.format, 42);
        String replay = sink.buildPartitionPath(record, sink.partitioner, sink.format, 42);
        assertNotEquals(first, replay);
        assertTrue(first.startsWith("public/default/iot/12.34.-1-"));
        assertTrue(first.endsWith(".parquet"));
    }

    @Test
    public void disabledModePreservesOriginalFileNames() throws Exception {
        config.remove("topicSchemaFallback");
        doCallRealMethod().when(sink).buildPartitionPath(any(), any(), any(), any(Long.class));
        sink.open(config, context);
        Record<GenericRecord> record = record("{\"id\":1}");
        when(record.getMessage().get().getMessageId()).thenReturn(new MessageIdImpl(12, 34, -1));
        assertEquals("public/default/iot/12.34.-1.parquet",
                sink.buildPartitionPath(record, sink.partitioner, sink.format, 42));
    }

    @Test
    public void concurrentCloseAndNormalizationDoesNotReportSystemFailure() throws Exception {
        sink.open(config, context);
        Record<GenericRecord> record = record("{\"id\":1}");
        GenericRecord value = record.getValue();
        CountDownLatch normalizing = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        when(record.getValue()).thenAnswer(call -> {
            normalizing.countDown();
            assertTrue(proceed.await(5, TimeUnit.SECONDS));
            return value;
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> writing = executor.submit(() -> {
                try {
                    sink.write(record);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(normalizing.await(5, TimeUnit.SECONDS));
            sink.close();
            proceed.countDown();
            writing.get(5, TimeUnit.SECONDS);
            verify(record.getMessage().get()).release();
            verify(record, never()).ack();
            verify(record, never()).fail();
            verify(context, never()).fatal(any());
        } finally {
            proceed.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void pauseResolvesActualPartitionIndicesAndNonPartitionedTopics() throws Exception {
        String other = TOPIC + "-other";
        when(context.getInputTopics()).thenReturn(List.of(TOPIC, other));
        when(context.getPulsarClient().getPartitionsForTopic(TOPIC, false)).thenReturn(
                CompletableFuture.completedFuture(List.of(TOPIC + "-partition-0", TOPIC + "-partition-1")));
        BlobStoreAbstractSink.pauseInputConsumers(context, 1000);
        verify(context).pause(TOPIC, 0);
        verify(context).pause(TOPIC, 1);
        verify(context).pause(other, -1);
        verify(context, never()).pause(TOPIC, -1);
        verify(context.getPulsarClient(), never()).close();
    }

    @Test
    public void pauseFailureDoesNotSkipRemainingPartitionsOrTopics() throws Exception {
        String other = TOPIC + "-other";
        when(context.getInputTopics()).thenReturn(List.of(TOPIC, other));
        when(context.getPulsarClient().getPartitionsForTopic(TOPIC, false)).thenReturn(
                CompletableFuture.completedFuture(List.of(TOPIC + "-partition-0", TOPIC + "-partition-1")));
        doThrow(new PulsarClientException("partition unavailable")).when(context).pause(TOPIC, 0);
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> BlobStoreAbstractSink.pauseInputConsumers(context, 1000));
        assertEquals(1, failure.getSuppressed().length);
        verify(context).pause(TOPIC, 1);
        verify(context).pause(other, -1);
    }

    @Test
    public void partitionLookupTimeoutIsBoundedAndOtherTopicsAreStillPaused() throws Exception {
        String other = TOPIC + "-other";
        when(context.getInputTopics()).thenReturn(List.of(TOPIC, other));
        when(context.getPulsarClient().getPartitionsForTopic(TOPIC, false)).thenReturn(new CompletableFuture<>());
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> BlobStoreAbstractSink.pauseInputConsumers(context, 10));
        assertTrue(failure.getSuppressed()[0] instanceof java.util.concurrent.TimeoutException);
        verify(context).pause(other, -1);
        verify(context.getPulsarClient(), never()).close();
    }

    @Test
    public void emptyOrDeniedPartitionLookupCannotReportSuccessfulPause() {
        for (CompletableFuture<List<String>> result : List.<CompletableFuture<List<String>>>of(
                CompletableFuture.completedFuture(List.of()),
                CompletableFuture.failedFuture(new SecurityException("denied")))) {
            when(context.getPulsarClient().getPartitionsForTopic(TOPIC, false)).thenReturn(result);
            assertThrows(IllegalStateException.class, () -> BlobStoreAbstractSink.pauseInputConsumers(context, 1000));
        }
    }

    @Test
    public void uploadPauseFailureIsReportedAsSystemFailureWithoutCompletingRecords() throws Exception {
        doThrow(new IOException("storage unavailable")).when(writer).uploadBlob(anyString(), any());
        doThrow(new PulsarClientException("cannot pause")).when(context).pause(TOPIC, -1);
        sink.open(config, context);
        Record<GenericRecord> first = record("{\"id\":1}");
        Record<GenericRecord> last = record("{\"id\":2}");
        sink.write(first);
        sink.write(last);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(context).fatal(any()));
        verify(context).recordMetric("_cloud_storage_sink_pause_failure_", 1);
        verify(context, never()).recordMetric("_cloud_storage_sink_paused_", 1);
        for (Record<GenericRecord> record : List.of(first, last)) {
            verify(record, never()).ack();
            verify(record, never()).fail();
            verify(record.getMessage().get()).release();
        }
    }
}
