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
package org.apache.pulsar.io.jcloud.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.cache.LoadingCache;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.common.functions.FunctionConfig.ProcessingGuarantees;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.format.ParquetFormat;
import org.apache.pulsar.io.jcloud.format.ParquetInputFile;
import org.apache.pulsar.io.jcloud.format.parquet.proto.Metadata;
import org.apache.pulsar.io.jcloud.schema.SchemalessJsonRecordNormalizer.MalformedRecordException;
import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;
import org.junit.Test;

public class TopicSchemaFallbackTest {
    public static final String TOPIC = "persistent://public/default/iot";
    public static final org.apache.avro.Schema NATIVE_SCHEMA = SchemaBuilder.record("Event").fields()
            .requiredInt("id").requiredLong("time").requiredDouble("value").requiredBoolean("active")
            .requiredString("name")
            .name("optional").type().nullable().stringType().noDefault()
            .name("child").type().record("Child").fields().requiredInt("count").endRecord().noDefault()
            .name("items").type().array().items().record("Item").fields().requiredString("s")
            .endRecord().noDefault()
            .name("tags").type().map().values().longType().noDefault().endRecord();
    public static final String VALID = "{\"id\":1,\"time\":9223372036854775807,\"value\":1.5,"
            + "\"active\":true,\"name\":\"iot\",\"child\":{\"count\":2},"
            + "\"items\":[{\"s\":\"x\"}],\"tags\":{\"a\":3}}";

    public static SchemaInfo info() {
        return SchemaInfo.builder().name("Event").type(SchemaType.JSON)
                .schema(NATIVE_SCHEMA.toString().getBytes(StandardCharsets.UTF_8)).properties(Map.of()).build();
    }

    @SuppressWarnings("unchecked")
    public static Record<GenericRecord> bytes(String topic, byte[] payload) {
        Record<GenericRecord> record = mock(Record.class);
        GenericRecord value = mock(GenericRecord.class);
        Message<GenericRecord> message = mock(Message.class);
        when(value.getSchemaType()).thenReturn(SchemaType.BYTES);
        when(value.getNativeObject()).thenReturn(payload);
        when(record.getValue()).thenReturn(value);
        when(record.getMessage()).thenReturn(Optional.of(message));
        when(record.getTopicName()).thenReturn(Optional.of(topic));
        when(record.getProperties()).thenReturn(Map.of("source", "iot"));
        when(record.getKey()).thenReturn(Optional.of("key"));
        when(record.getEventTime()).thenReturn(Optional.of(123L));
        when(record.getPartitionId()).thenReturn(Optional.of("0"));
        when(record.getPartitionIndex()).thenReturn(Optional.of(0));
        when(record.getRecordSequence()).thenReturn(Optional.of(1L));
        when(record.getDestinationTopic()).thenReturn(Optional.of("destination"));
        when(message.size()).thenReturn(payload == null ? 0 : payload.length);
        return record;
    }

    @SuppressWarnings("unchecked")
    public static SinkContext context(SchemaInfo schemaInfo, long version) throws Exception {
        SinkContext context = mock(SinkContext.class);
        SinkConfig config = new SinkConfig();
        config.setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE);
        config.setAutoAck(false);
        when(context.getSinkConfig()).thenReturn(config);
        when(context.getInputTopics()).thenReturn(List.of(TOPIC));
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        LookupService lookup = mock(LookupService.class);
        when(context.getPulsarClient()).thenReturn(client);
        when(client.getLookup(any(String.class))).thenReturn(lookup);
        when(client.getLookup()).thenReturn(lookup);
        LoadingCache<String, SchemaInfoProvider> cache = mock(LoadingCache.class);
        when(client.getSchemaProviderLoadingCache()).thenReturn(cache);
        when(cache.get(any(String.class))).thenAnswer(call ->
                new MultiVersionSchemaInfoProvider(TopicName.get((String) call.getArgument(0)), client));
        when(lookup.getSchema(any(TopicName.class), any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(schemaInfo)));
        return context;
    }

    private SchemalessJsonRecordNormalizer normalizer() {
        return new SchemalessJsonRecordNormalizer(info(), "fallback:" + TOPIC + ":7", 1024);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void parquetRoundTripAndOriginalMetadata() throws Exception {
        Record<GenericRecord> original = bytes(TOPIC + "-partition-0", VALID.getBytes(StandardCharsets.UTF_8));
        ResolvedSchemaRecord resolved = normalizer().normalize(original);
        assertEquals(SchemaType.JSON, resolved.getValue().getSchemaType());
        assertSame(resolved.getSchema(), AvroRecordUtil.getPulsarSchema(resolved));
        assertSame(original.getMessage().get(), resolved.getMessage().get());
        assertNull(resolved.getMessage().get().getSchemaVersion());
        assertEquals(original.getTopicName(), resolved.getTopicName());
        assertEquals(original.getKey(), resolved.getKey());
        assertEquals(original.getProperties(), resolved.getProperties());
        assertEquals(original.getEventTime(), resolved.getEventTime());
        assertEquals(original.getPartitionIndex(), resolved.getPartitionIndex());
        assertEquals(original.getPartitionId(), resolved.getPartitionId());
        assertEquals(original.getRecordSequence(), resolved.getRecordSequence());
        assertEquals(original.getDestinationTopic(), resolved.getDestinationTopic());
        ParquetFormat format = new ParquetFormat();
        BlobStoreAbstractConfig config = new BlobStoreAbstractConfig();
        config.setParquetCodec(null);
        format.configure(config);
        // Switching away from a typed Protobuf group must clear the previous writer descriptor.
        format.initSchema((Schema) Schema.PROTOBUF_NATIVE(Metadata.PulsarIOCSCProtobufMessageMetadata.class));
        format.initSchema(resolved.getSchema());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(format.recordWriterBuf(List.<Record<GenericRecord>>of(resolved).iterator()).array());
        try (ParquetReader<org.apache.avro.generic.GenericRecord> reader = AvroParquetReader
                .<org.apache.avro.generic.GenericRecord>builder(new ParquetInputFile("fallback.parquet", output))
                .withDataModel(GenericData.get()).build()) {
            org.apache.avro.generic.GenericRecord read = reader.read();
            assertEquals(1, read.get("id"));
            assertEquals(Long.MAX_VALUE, read.get("time"));
            assertEquals(1.5, read.get("value"));
            assertEquals(true, read.get("active"));
            assertEquals("iot", read.get("name").toString());
            assertNull(read.get("optional"));
            assertEquals(2, ((org.apache.avro.generic.GenericRecord) read.get("child")).get("count"));
            assertEquals("x", ((org.apache.avro.generic.GenericRecord) ((List<?>) read.get("items")).get(0))
                    .get("s").toString());
            assertEquals(3L, ((Map<?, ?>) read.get("tags")).values().iterator().next());
            assertNull(reader.read());
        }
        resolved.ack();
        resolved.fail();
        resolved.releaseUnacknowledged();
        verify(original, times(1)).ack();
        verify(original, never()).fail();
        verify(original.getMessage().get(), never()).release();
    }

    @Test
    public void malformedIsRejectedBeforeBatching() {
        List<String> invalid = List.of("null", "[]", "1", "{}", "not JSON", VALID + " {}",
                VALID.replace("\"id\":1", "\"id\":2147483648"),
                VALID.replace("\"id\":1", "\"id\":1.1"),
                VALID.replace("\"id\":1", "\"id\":\"1\""),
                VALID.replace("9223372036854775807", "9223372036854775808"),
                VALID.replace("1.5", "1e400"),
                VALID.replace("\"iot\"", "null"),
                VALID.replace("{\"count\":2}", "{}"),
                VALID.replace("{\"s\":\"x\"}", "{\"s\":1}"),
                VALID.replace("{\"a\":3}", "{\"a\":\"3\"}"),
                VALID.replace("\"id\":1", "\"extra\":1,\"id\":1"),
                VALID.replace("\"id\":1", "\"id\":1,\"id\":2"));
        for (String payload : invalid) {
            MalformedRecordException e = assertThrows(MalformedRecordException.class,
                    () -> normalizer().normalize(bytes(TOPIC, payload.getBytes(StandardCharsets.UTF_8))));
            assertTrue(e.getMessage().length() < 80);
            assertNull(e.getCause());
        }
        for (byte[] payload : new byte[][]{null, new byte[0], new byte[]{(byte) 0xc3, 0x28},
                new byte[]{0, 1, 2}, new byte[1025]}) {
            assertThrows(MalformedRecordException.class, () -> normalizer().normalize(bytes(TOPIC, payload)));
        }
    }

    @Test
    public void explicitPinAcrossPartitionsRestartsAndInstances() throws Exception {
        TopicSchemaFallbackConfig config = new TopicSchemaFallbackConfig();
        config.setTopicVersions(Map.of(TOPIC, 7L));
        SinkContext context = context(info(), 7);
        for (int instance = 0; instance < 2; instance++) {
            TopicSchemaResolver resolver = new TopicSchemaResolver(config, context);
            for (int retry = 0; retry < 3; retry++) {
                Record<GenericRecord> record = bytes(TOPIC + "-partition-" + retry,
                        VALID.getBytes(StandardCharsets.UTF_8));
                assertEquals(Long.MAX_VALUE, resolver.forRecord(record).normalize(record).getAvroRecord().get("time"));
            }
        }
        LookupService lookup = ((PulsarClientImpl) context.getPulsarClient()).getLookup(TOPIC);
        verify(lookup, times(2)).getSchema(eq(TopicName.get(TOPIC)),
                eq(ByteBuffer.allocate(8).putLong(7).array()));
        verify(lookup, never()).getSchema(any(TopicName.class));
        verify(context.getPulsarClient(), never()).close();
        TopicSchemaResolver resolver = new TopicSchemaResolver(config, context);
        Record<GenericRecord> versioned = bytes(TOPIC, VALID.getBytes(StandardCharsets.UTF_8));
        when(versioned.getMessage().get().getSchemaVersion()).thenReturn(new byte[]{1});
        assertNull(resolver.forRecord(versioned));
        Record<GenericRecord> typed = bytes(TOPIC, new byte[0]);
        when(typed.getValue().getSchemaType()).thenReturn(SchemaType.JSON);
        assertNull(resolver.forRecord(typed));
        assertNull(resolver.forRecord(bytes(TOPIC + "other", new byte[0])));
    }

    @Test
    public void missingPermissionAndTimeoutAreStartupFailures() throws Exception {
        TopicSchemaFallbackConfig config = new TopicSchemaFallbackConfig();
        config.setTopicVersions(Map.of(TOPIC, 0L));
        config.setLookupTimeoutMs(10);
        SinkContext context = context(info(), 0);
        LookupService lookup = ((PulsarClientImpl) context.getPulsarClient()).getLookup(TOPIC);
        when(lookup.getSchema(any(TopicName.class), any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new TopicSchemaResolver(config, context));
        when(lookup.getSchema(any(TopicName.class), any(byte[].class)))
                .thenReturn(CompletableFuture.failedFuture(new SecurityException("denied")));
        assertThrows(IllegalArgumentException.class, () -> new TopicSchemaResolver(config, context));
        when(lookup.getSchema(any(TopicName.class), any(byte[].class))).thenReturn(new CompletableFuture<>());
        assertThrows(IllegalArgumentException.class, () -> new TopicSchemaResolver(config, context));
        verify(context, times(3)).recordMetric("_cloud_storage_schema_lookup_failure_", 1);
    }

    @Test
    public void configurationRejectsUnsafeCombinations() throws Exception {
        BlobStoreAbstractConfig config = new BlobStoreAbstractConfig();
        assertFalse(config.getTopicSchemaFallback().isEnabled());
        assertEquals(BlobStoreAbstractConfig.SchemalessDecodeFailureAction.REDELIVER,
                config.getSchemalessDecodeFailureAction());
        config.getTopicSchemaFallback().setEnabled(true);
        assertThrows(IllegalArgumentException.class, config::validate);
        config.setFormatType("parquet").setSkipFailedMessages(true);
        assertThrows(IllegalArgumentException.class, config::validate);
        TopicSchemaFallbackConfig fallback = config.getTopicSchemaFallback();
        fallback.setTopicVersions(Map.of("iot", 0L));
        assertThrows(IllegalArgumentException.class, fallback::validate);
        fallback.setTopicVersions(Map.of(TOPIC + "-partition-0", 0L));
        assertThrows(IllegalArgumentException.class, fallback::validate);
        fallback.setTopicVersions(Map.of(TOPIC, -1L));
        assertThrows(IllegalArgumentException.class, fallback::validate);
        fallback.setTopicVersions(Map.of(TOPIC, 0L));
        fallback.validate();
        SinkContext context = context(info(), 0);
        for (ProcessingGuarantees guarantee : ProcessingGuarantees.values()) {
            if (guarantee != ProcessingGuarantees.ATLEAST_ONCE) {
                context.getSinkConfig().setProcessingGuarantees(guarantee);
                assertThrows(IllegalArgumentException.class, () -> new TopicSchemaResolver(fallback, context));
            }
        }
        context.getSinkConfig().setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE);
        context.getSinkConfig().setAutoAck(true);
        assertThrows(IllegalArgumentException.class, () -> new TopicSchemaResolver(fallback, context));
        context.getSinkConfig().setAutoAck(false);
        context.getSinkConfig().setTimeoutMs(1000L);
        assertThrows(IllegalArgumentException.class, () -> new TopicSchemaResolver(fallback, context));
    }

    @Test
    public void unsupportedSchemaRejectedAtStartup() {
        SchemaInfo avro = SchemaInfo.builder().type(SchemaType.AVRO).schema(info().getSchema()).build();
        TopicSchemaFallbackConfig config = new TopicSchemaFallbackConfig();
        config.setTopicVersions(Map.of(TOPIC, 0L));
        assertThrows(IllegalArgumentException.class, () -> new TopicSchemaResolver(config, context(avro, 0)));
        for (String type : List.of("bytes", "[\"int\",\"string\"]",
                "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}")) {
            String definition = "{\"type\":\"record\",\"name\":\"Bad\",\"fields\":[{\"name\":\"x\",\"type\":"
                    + (type.equals("bytes") ? "\"bytes\"" : type) + "}]}";
            SchemaInfo bad = SchemaInfo.builder().type(SchemaType.JSON)
                    .schema(definition.getBytes(StandardCharsets.UTF_8)).build();
            assertThrows(IllegalArgumentException.class,
                    () -> new SchemalessJsonRecordNormalizer(bad, "bad", 1024));
        }
    }

    @Test
    public void nullableWireVersionRemainsNullInParquetMetadata() throws Exception {
        for (boolean humanReadable : List.of(false, true)) {
            Record<GenericRecord> original = bytes(TOPIC, VALID.getBytes(StandardCharsets.UTF_8));
            when(original.getMessage().get().getMessageId()).thenReturn(new MessageIdImpl(1, 2, 0));
            when(original.getMessage().get().getProperties()).thenReturn(Map.of("source", "iot"));
            ResolvedSchemaRecord resolved = normalizer().normalize(original);
            ParquetFormat format = new ParquetFormat();
            BlobStoreAbstractConfig config = new BlobStoreAbstractConfig().setWithMetadata(true)
                    .setUseHumanReadableSchemaVersion(humanReadable).setParquetCodec(null);
            format.configure(config);
            format.initSchema(resolved.getSchema());
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            output.write(format.recordWriterBuf(List.<Record<GenericRecord>>of(resolved).iterator()).array());
            try (ParquetReader<org.apache.avro.generic.GenericRecord> reader = AvroParquetReader
                    .<org.apache.avro.generic.GenericRecord>builder(new ParquetInputFile("metadata.parquet", output))
                    .withDataModel(GenericData.get()).build()) {
                org.apache.avro.generic.GenericRecord metadata =
                        (org.apache.avro.generic.GenericRecord) reader.read().get("__message_metadata__");
                assertNull(metadata.get("schemaVersion"));
                assertEquals("iot", ((Map<?, ?>) metadata.get("properties")).values().iterator().next().toString());
            }
            assertNull(original.getMessage().get().getSchemaVersion());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void normalizedSchemaOverridesInternalConsumerSchema() throws Exception {
        Record<GenericRecord> original = bytes(TOPIC, VALID.getBytes(StandardCharsets.UTF_8));
        MessageImpl<GenericRecord> message = mock(MessageImpl.class);
        SchemaInfo different = SchemaInfo.builder().type(SchemaType.JSON)
                .schema(SchemaBuilder.record("Different").fields().requiredString("other").endRecord()
                        .toString().getBytes(StandardCharsets.UTF_8)).build();
        when(message.getSchemaInternal()).thenReturn(Schema.generic(different));
        when(original.getMessage()).thenReturn(Optional.of(message));
        ResolvedSchemaRecord normalized = normalizer().normalize(original);
        assertEquals(info().getSchemaDefinition(), AvroRecordUtil.getPulsarSchema(normalized)
                .getSchemaInfo().getSchemaDefinition());
    }

    @Test
    public void fractionalStringAndOutOfRangePinsCannotBeCoerced() {
        for (Object version : List.of(1.5, "3", new java.math.BigInteger("9223372036854775808"))) {
            assertThrows(java.io.IOException.class, () -> CloudStorageSinkConfig.load(Map.of(
                    "topicSchemaFallback", Map.of("enabled", true, "topicVersions", Map.of(TOPIC, version)))));
        }
    }

    @Test
    public void floatingPointOverflowAndUnderflowAreRecordErrors() {
        for (String type : List.of("float", "double")) {
            String definition = "{\"type\":\"record\",\"name\":\"Number\",\"fields\":[{\"name\":\"x\",\"type\":\""
                    + type + "\"}]}";
            SchemaInfo schema = SchemaInfo.builder().type(SchemaType.JSON)
                    .schema(definition.getBytes(StandardCharsets.UTF_8)).build();
            SchemalessJsonRecordNormalizer normalizer = new SchemalessJsonRecordNormalizer(schema, type, 1024);
            for (String value : List.of("1e400", "1e-400", "\"1\"", "null", "true")) {
                assertThrows(MalformedRecordException.class, () -> normalizer.normalize(bytes(TOPIC,
                        ("{\"x\":" + value + "}").getBytes(StandardCharsets.UTF_8))));
            }
        }
    }

    @Test
    public void unacknowledgedReleaseIsTerminal() {
        Record<GenericRecord> original = bytes(TOPIC, new byte[0]);
        ResolvedSchemaRecord record = new ResolvedSchemaRecord(original);
        record.releaseUnacknowledged();
        record.releaseUnacknowledged();
        record.ack();
        record.fail();
        verify(original.getMessage().get(), times(1)).release();
        verify(original, never()).ack();
        verify(original, never()).fail();
        assertTrue(record.getMessage().isPresent());
    }
}
