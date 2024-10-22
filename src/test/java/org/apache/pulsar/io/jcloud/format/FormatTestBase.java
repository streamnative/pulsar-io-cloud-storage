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
package org.apache.pulsar.io.jcloud.format;

import static org.junit.Assert.fail;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.schema.proto.Test.TestMessage;
import org.apache.pulsar.io.jcloud.util.MetadataUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * parquet format test.
 */
public abstract class FormatTestBase extends PulsarTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(FormatTestBase.class);

    private static final TopicName avroTopicName =
            TopicName.get("test-parquet-avro" + RandomStringUtils.randomAlphabetic(5));
    private static final TopicName jsonTopicName =
            TopicName.get("test-parquet-json" + RandomStringUtils.randomAlphabetic(5));
    private static final TopicName kvTopicName =
            TopicName.get("test-parquet-kv" + RandomStringUtils.randomAlphabetic(5));
    private static final TopicName protobufNativeTopicName =
            TopicName.get("test-parquet-protobuf-native" + RandomStringUtils.randomAlphabetic(5));
    private static final TopicName kvSeparatedTopicName =
            TopicName.get("test-parquet-kv-sep" + RandomStringUtils.randomAlphabetic(5));
    protected static TopicName jsonBytesTopicName =
            TopicName.get("test-json-bytes-parquet-json" + RandomStringUtils.randomAlphabetic(5));
    protected static TopicName jsonStringTopicName =
            TopicName.get("test-json-string-parquet-json" + RandomStringUtils.randomAlphabetic(5));

    @BeforeClass
    public static void setUp() throws Exception {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getAdminUrl())
                .build();
        pulsarAdmin.topics().createPartitionedTopic(jsonTopicName.toString(), 1);
        pulsarAdmin.topics().createSubscription(jsonTopicName.toString(), "test", MessageId.earliest);
        pulsarAdmin.topics().createPartitionedTopic(kvTopicName.toString(), 1);
        pulsarAdmin.topics().createSubscription(kvTopicName.toString(), "test", MessageId.earliest);
        pulsarAdmin.topics().createPartitionedTopic(avroTopicName.toString(), 1);
        pulsarAdmin.topics().createSubscription(avroTopicName.toString(), "test", MessageId.earliest);
        pulsarAdmin.topics().createPartitionedTopic(protobufNativeTopicName.toString(), 1);
        pulsarAdmin.topics().createSubscription(protobufNativeTopicName.toString(), "test", MessageId.earliest);
        pulsarAdmin.topics().createPartitionedTopic(jsonBytesTopicName.toString(), 1);
        pulsarAdmin.topics().createSubscription(jsonBytesTopicName.toString(), "test", MessageId.earliest);
        pulsarAdmin.topics().createPartitionedTopic(jsonStringTopicName.toString(), 1);
        pulsarAdmin.topics().createSubscription(jsonStringTopicName.toString(), "test", MessageId.earliest);
    }

    public abstract Format<GenericRecord> getFormat();

    public abstract String expectedFormatExtension();

    @Test
    public void testGetExtension() {
        Assert.assertEquals(expectedFormatExtension(), getFormat().getExtension());
    }

    @Test
    public void testAvroRecordWriter() throws Exception {
        List<TestRecord> testRecords = Arrays.asList(
                new TestRecord("key1", 1, null),
                new TestRecord("key1", 1, new TestRecord.TestSubRecord("aaa")),
                new TestRecord("key2", 2, new TestRecord.TestSubRecord("aaa"))
        );

        sendTypedMessages(avroTopicName.toString(), SchemaType.AVRO, testRecords, Optional.empty(), TestRecord.class);

        Consumer<Message<GenericRecord>> handle = getMessageConsumer(avroTopicName);
        consumerMessages(avroTopicName.toString(), Schema.AUTO_CONSUME(), handle, testRecords.size(), 2000);
    }

    @Test
    public void testJsonRecordWriter() throws Exception {
        List<TestRecord> testRecords = Arrays.asList(
                new TestRecord("key1", 1, null),
                new TestRecord("key1", 1, new TestRecord.TestSubRecord("aaa"))
        );

        sendTypedMessages(jsonTopicName.toString(), SchemaType.JSON, testRecords, Optional.empty(), TestRecord.class);

        Consumer<Message<GenericRecord>>
                handle = getJSONMessageConsumer(jsonTopicName);
        consumerMessages(jsonTopicName.toString(), Schema.AUTO_CONSUME(), handle, testRecords.size(), 2000);
    }


    @Test
    public void testKeyValueRecordWriter() throws Exception {
        KeyValue<TestRecord, TestRecord> kv1 = new KeyValue<>(
                new TestRecord("key1", 1, null),
                new TestRecord("value1", 1, null)
        );
        KeyValue<TestRecord, TestRecord> kv2 = new KeyValue<>(
                new TestRecord("key1", 1, new TestRecord.TestSubRecord("aaa")),
                new TestRecord("value1", 1, new TestRecord.TestSubRecord("aaa"))
        );
        List<KeyValue> testRecords = Arrays.asList(
                kv1,
                kv2
        );

        sendTypedMessages(kvTopicName.toString(), SchemaType.KEY_VALUE,
                testRecords, Optional.empty(), KeyValue.class);

        Consumer<Message<GenericRecord>>
                handle = getJSONMessageConsumer(kvTopicName);
        consumerMessages(kvTopicName.toString(), Schema.AUTO_CONSUME(), handle, testRecords.size(), 2000);
    }

    @Test
    public void testKeyValueAvroWithSameNamespaceName() throws Exception {
        org.apache.avro.Schema keySchema = SchemaBuilder.record("record")
                .fields()
                .name("id").type().stringType().noDefault()
                .endRecord();
        org.apache.avro.Schema valueSchema = SchemaBuilder.record("record")
                .fields()
                .name("content").type().stringType().noDefault()
                .endRecord();

        SchemaDefinition<Object> keySchemaDef = SchemaDefinition.builder()
                .withJsonDef(String.format(""
                        + "{\n"
                        + "                            \t\"type\": \"record\",\n"
                        + "                            \t\"name\": \"record\",\n"
                        + "                            \t\"fields\": [{\n"
                        + "                            \t\t\"name\": \"id\",\n"
                        + "                            \t\t\"type\": [\"string\"]\n"
                        + "                            \t}]\n"
                        + "                            }"))
                .build();

        SchemaDefinition<Object> valueSchemaDef = SchemaDefinition.builder()
                .withJsonDef(String.format(""
                        + "{\n"
                        + "                            \t\"type\": \"record\",\n"
                        + "                            \t\"name\": \"record\",\n"
                        + "                            \t\"fields\": [{\n"
                        + "                            \t\t\"name\": \"content\",\n"
                        + "                            \t\t\"type\": [\"string\"]\n"
                        + "                            \t}]\n"
                        + "                            }"))
                .build();


        Schema<KeyValue<Object, Object>> schema = Schema.KeyValue(Schema.AVRO(keySchemaDef),
                Schema.AVRO(valueSchemaDef), KeyValueEncodingType.SEPARATED);

        GenericData.Record keyRecord = new GenericData.Record(keySchema);
        keyRecord.put("id", "theid");
        GenericData.Record valueRecord = new GenericData.Record(valueSchema);
        valueRecord.put("content", "ccc");
        List<KeyValue> testRecords = Arrays.asList(
                new KeyValue(keyRecord, valueRecord),
                new KeyValue(keyRecord, null)
        );

        sendTypedMessages(kvSeparatedTopicName.toString(), testRecords, schema, Optional.empty());

        Consumer<Message<GenericRecord>> handle = getMessageConsumer(kvSeparatedTopicName);
        consumerMessages(kvSeparatedTopicName.toString(), Schema.AUTO_CONSUME(), handle, testRecords.size(), 2000);
    }

    @Test
    public void testProtobufNativeRecordWriter() throws Exception {
        List<TestMessage> testRecords = Arrays.asList(
                TestMessage.newBuilder().setStringField("key1").setIntField(1).build(),
                TestMessage.newBuilder().setStringField("key2").setIntField(2)
                        .putStringMap("foo", "bar").putStringMap("a", "b").build()
        );

        sendProtobufNativeMessages(protobufNativeTopicName.toString(), SchemaType.PROTOBUF_NATIVE,
                testRecords, Optional.empty(), TestMessage.class);

        Consumer<Message<GenericRecord>>
                handle = getProtobufNativeMessageConsumer(protobufNativeTopicName);
        consumerMessages(protobufNativeTopicName.toString(), Schema.AUTO_CONSUME(), handle, testRecords.size(), 2000);
    }

    protected abstract boolean supportMetadata();

    protected Consumer<Message<GenericRecord>> getMessageConsumer(TopicName topic) {
        return msg -> {
            try {
                Schema<GenericRecord> schema = (Schema<GenericRecord>) msg.getReaderSchema().get();
                initSchema(schema);
                org.apache.avro.generic.GenericRecord formatGeneratedRecord = getFormatGeneratedRecord(topic, msg);
                assertEquals(msg.getValue(), formatGeneratedRecord);
                if (supportMetadata()) {
                    validMetadata(formatGeneratedRecord, msg);
                }
            } catch (Exception e) {
                LOGGER.error("formatter handle message is fail", e);
                fail();
            }
        };
    }

    protected Consumer<Message<GenericRecord>> getJSONMessageConsumer(TopicName topic) {
        return msg -> {
            try {
                Schema<GenericRecord> schema = (Schema<GenericRecord>) msg.getReaderSchema().get();
                initSchema(schema);
                Map<String, Object> message = getJSONMessage(topic, msg);
                if (message != null) {
                    Assert.assertFalse(message.isEmpty());
                }
                // TODO: do more check
            } catch (Exception e) {
                LOGGER.error("formatter handle message is fail", e);
                fail();
            }
        };
    }

    protected Consumer<Message<GenericRecord>> getProtobufNativeMessageConsumer(TopicName topic) {
        return msg -> {
            try {
                Schema<GenericRecord> schema = (Schema<GenericRecord>) msg.getReaderSchema().get();
                initSchema(schema);
                DynamicMessage dynamicMessage = getDynamicMessage(topic, msg);
                Assert.assertEquals(msg.getValue().getNativeObject().toString(), dynamicMessage.toString());
            } catch (Exception e) {
                LOGGER.error("formatter handle message is fail", e);
                fail();
            }
        };
    }

    public abstract org.apache.avro.generic.GenericRecord getFormatGeneratedRecord(TopicName topicName,
                                                                                   Message<GenericRecord> msg)
            throws Exception;

    public abstract DynamicMessage getDynamicMessage(TopicName topicName,
                                                            Message<GenericRecord> msg)
            throws Exception;

    public abstract Map<String, Object> getJSONMessage(TopicName topicName,
                                                     Message<GenericRecord> msg)
            throws Exception;

    protected void assertEquals(DynamicMessage msgValue, org.apache.avro.generic.GenericRecord record) {
        Descriptors.Descriptor descriptor = msgValue.getDescriptorForType();
        for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            Object sourceValue = msgValue.getField(descriptor.findFieldByName(field.name()));
            Object newValue = record.get(field.name());
            Assert.assertEquals(
                MessageFormat.format(
                        "field[{0} sourceValue [{1}:{2}] not equal newValue [{3}:{4}]",
                        field.name(),
                        sourceValue,
                        sourceValue != null ? sourceValue.getClass().getName() : "null",
                        newValue,
                        newValue != null ? newValue.getClass().getName() : "null"
                ),
                sourceValue,
                newValue);
        }
    }

    protected void assertEquals(GenericRecord msgValue, org.apache.avro.generic.GenericRecord record) {
        for (Field field : msgValue.getFields()) {
            Object sourceValue = getField(msgValue, field);
            Object newValue = record.get(field.getName());
            if (newValue instanceof Utf8) {
                newValue = ((Utf8) newValue).toString();
            }
            if (sourceValue instanceof GenericRecord && newValue instanceof org.apache.avro.generic.GenericRecord) {
                assertEquals((GenericRecord) sourceValue, (org.apache.avro.generic.GenericRecord) newValue);
            } else if (record.getSchema().getField(field.getName()).schema().getType()
                    == org.apache.avro.Schema.Type.ENUM) {
                Assert.assertNotNull(sourceValue);
                Assert.assertNotNull(newValue);
                Assert.assertEquals(sourceValue.toString(), newValue.toString());
            } else if (sourceValue instanceof DynamicMessage
                    && newValue instanceof org.apache.avro.generic.GenericRecord) {
                assertEquals((DynamicMessage) sourceValue, (org.apache.avro.generic.GenericRecord) newValue);
            } else if (record.getSchema().getField(field.getName()).schema().getType()
                    == org.apache.avro.Schema.Type.ARRAY) {
                if (sourceValue instanceof List && newValue instanceof List) {
                    Assert.assertEquals(((List<?>) sourceValue).size(), ((List<?>) newValue).size());
                }
            } else {
                Assert.assertEquals(
                        MessageFormat.format(
                                "field[{0} sourceValue [{1}:{2}] not equal newValue [{3}:{4}]",
                                field.getName(),
                                sourceValue,
                                sourceValue != null ? sourceValue.getClass().getName() : "null",
                                newValue,
                                newValue != null ? newValue.getClass().getName() : "null"
                        ),
                        sourceValue,
                        newValue);
            }
        }
    }

    private Object getField(GenericRecord recordValue, Field field) {
        if (recordValue instanceof GenericJsonRecord) {
            try {
                return recordValue.getField(field);
            } catch (NullPointerException ignore) {
                return null;
            }
        }
        return recordValue.getField(field);
    }

    protected BlobStoreAbstractConfig getBlobStoreAbstractConfig() {
        BlobStoreAbstractConfig config = new BlobStoreAbstractConfig();
        if (supportMetadata()) {
            config.setWithMetadata(true);
        }
        return config;
    }

    protected void initSchema(Schema<GenericRecord> schema) {
        Schema<GenericRecord> convertSchema = schema;
        if (schema instanceof AutoConsumeSchema) {
            AutoConsumeSchema autoConsumeSchema = (AutoConsumeSchema) schema;
            convertSchema = (Schema<GenericRecord>) autoConsumeSchema.atSchemaVersion(null);
        }
        final BlobStoreAbstractConfig config = getBlobStoreAbstractConfig();
        ((InitConfiguration<BlobStoreAbstractConfig>) getFormat()).configure(config);
        Assert.assertTrue(getFormat().doSupportPulsarSchemaType(convertSchema.getSchemaInfo().getType()));
        getFormat().initSchema(convertSchema);
    }

    protected void validMetadata(org.apache.avro.generic.GenericRecord record, Message<GenericRecord> message) {
        LOGGER.info("validMetadata record {} GenericRecord {} fields {}",
                record, message.getValue(), message.getValue().getFields());
        Object object = record.get(MetadataUtil.MESSAGE_METADATA_KEY);
        Assert.assertNotNull(object);
        Assert.assertTrue(object instanceof org.apache.avro.generic.GenericRecord);
        org.apache.avro.generic.GenericRecord genericRecord = (org.apache.avro.generic.GenericRecord) object;
        Assert.assertEquals(genericRecord.getSchema(), MetadataUtil.MESSAGE_METADATA);
        Assert.assertNotNull(genericRecord.get(MetadataUtil.METADATA_MESSAGE_ID_KEY));

        Assert.assertEquals(ByteBuffer.wrap(message.getMessageId().toByteArray()),
                genericRecord.get(MetadataUtil.METADATA_MESSAGE_ID_KEY));
        Assert.assertNotNull(genericRecord.get(MetadataUtil.METADATA_PROPERTIES_KEY));
        Map<Utf8, Utf8> utf8Utf8Map = (Map<Utf8, Utf8>) genericRecord.get(MetadataUtil.METADATA_PROPERTIES_KEY);
        Map<String, String> resultMap = utf8Utf8Map.entrySet().stream().collect(
                Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().toString()
                )
        );
        Assert.assertTrue(
                mapsAreEqual(
                        message.getProperties(),
                        resultMap
                ));
        Assert.assertFalse(
                mapsAreEqual(
                        message.getReaderSchema().get().getSchemaInfo().getProperties(),
                        resultMap
                ));
        Assert.assertNotNull(genericRecord.get(MetadataUtil.METADATA_SCHEMA_VERSION_KEY));
        Assert.assertEquals(ByteBuffer.wrap(message.getSchemaVersion()),
                genericRecord.get(MetadataUtil.METADATA_SCHEMA_VERSION_KEY));
    }

    public boolean mapsAreEqual(Map<String, String> mapA, Map<String, String> mapB) {

        try {
            for (String k : mapB.keySet()) {
                if (!mapA.get(k).equals(mapB.get(k))) {
                    return false;
                }
            }
            for (String y : mapA.keySet()) {
                if (!mapB.containsKey(y)) {
                    return false;
                }
            }
        } catch (NullPointerException np) {
            return false;
        }
        return true;
    }

    protected void assertEquals(DynamicMessage msgValue, Map<String, Object> record) {
        Descriptors.Descriptor descriptor = msgValue.getDescriptorForType();
        for (String fieldName : record.keySet()) {
            Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
            if (fieldDescriptor != null) {
                Object sourceValue = msgValue.getField(fieldDescriptor);
                Object newValue = record.get(fieldName);
                if (!fieldDescriptor.isRepeated()) {
                    Assert.assertEquals(
                        MessageFormat.format(
                                "field[{0} sourceValue [{1}:{2}] not equal newValue [{3}:{4}]",
                                fieldName,
                                sourceValue,
                                sourceValue != null ? sourceValue.getClass().getName() : "null",
                                newValue,
                                newValue != null ? newValue.getClass().getName() : "null"
                        ),
                        sourceValue,
                        newValue);
                }
            }
        }
    }
}