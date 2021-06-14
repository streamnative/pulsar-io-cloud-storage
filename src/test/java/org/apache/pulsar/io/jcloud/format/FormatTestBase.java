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

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.PulsarTestBase;
import org.apache.pulsar.io.jcloud.bo.TestRecord;
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

    private static TopicName avroTopicName = TopicName.get("test-parquet-avro" + RandomStringUtils.random(5));
    private static TopicName jsonTopicName = TopicName.get("test-parquet-json" + RandomStringUtils.random(5));

    @BeforeClass
    public static void setUp() throws Exception {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getAdminUrl())
                .build();
        pulsarAdmin.topics().createPartitionedTopic(jsonTopicName.toString(), 1);
        pulsarAdmin.topics().createSubscription(jsonTopicName.toString(), "test", MessageId.earliest);
        pulsarAdmin.topics().createPartitionedTopic(avroTopicName.toString(), 1);
        pulsarAdmin.topics().createSubscription(avroTopicName.toString(), "test", MessageId.earliest);
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

        sendTypedMessages(jsonTopicName.toString(), SchemaType.AVRO, testRecords, Optional.empty(), TestRecord.class);

        Consumer<Message<GenericRecord>>
                handle = getMessageConsumer(jsonTopicName);
        consumerMessages(jsonTopicName.toString(), Schema.AUTO_CONSUME(), handle, testRecords.size(), 2000);
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
                Assert.fail();
            }
        };
    }

    public abstract org.apache.avro.generic.GenericRecord getFormatGeneratedRecord(TopicName topicName,
                                                                                   Message<GenericRecord> msg)
            throws Exception;

    protected void assertEquals(GenericRecord msgValue, org.apache.avro.generic.GenericRecord record) {
        for (Field field : msgValue.getFields()) {
            Object sourceValue = getField(msgValue, field);
            Object newValue = record.get(field.getName());
            if (newValue instanceof Utf8) {
                newValue = ((Utf8) newValue).toString();
            }
            if (sourceValue instanceof GenericRecord && newValue instanceof org.apache.avro.generic.GenericRecord) {
                assertEquals((GenericRecord) sourceValue, (org.apache.avro.generic.GenericRecord) newValue);
            } else {
                Assert.assertEquals(
                        MessageFormat.format(
                                "field[{0} sourceValue [{1}] not equal newValue [{2}]",
                                field.getName(),
                                sourceValue,
                                newValue
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

    protected void initSchema(Schema<GenericRecord> schema) {
        final BlobStoreAbstractConfig config = new BlobStoreAbstractConfig();
        if (supportMetadata()) {
            config.setWithMetadata(true);
        }
        ((InitConfiguration<BlobStoreAbstractConfig>) getFormat()).configure(config);
        getFormat().initSchema(schema);
    }

    protected void validMetadata(org.apache.avro.generic.GenericRecord record, Message<GenericRecord> message) {
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
}