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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.bo.TestRecord;
import org.apache.pulsar.jcloud.shade.com.google.common.io.ByteSource;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * json format test.
 */
public class JsonFormatTest extends FormatTestBase {
    private static final ThreadLocal<ObjectMapper> JSON_MAPPER = ThreadLocal.withInitial(() -> {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    });

    private static final Logger log = LoggerFactory.getLogger(JsonFormatTest.class);
    private JsonFormat format = new JsonFormat();

    @Test
    public void testJsonBytesRecordWriter() {
        List<TestRecord> testRecords = Arrays.asList(
                new TestRecord("key1", 1, null),
                new TestRecord("key1", 1, new TestRecord.TestSubRecord("aaa")),
                new TestRecord("key2", 2, new TestRecord.TestSubRecord("aaa"))
        );

        try {
            String json = JSON_MAPPER.get().writeValueAsString(testRecords);
            sendTypedMessages(jsonBytesTopicName.toString(), SchemaType.BYTES,
                    Arrays.asList(json.getBytes(), json.getBytes()), Optional.empty(), byte[].class);

            Consumer<Message<GenericRecord>> handle = getJsonByteAndStringHandler(jsonBytesTopicName);
            consumerMessages(jsonBytesTopicName.toString(), Schema.AUTO_CONSUME(), handle, 2, 2000);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testJsonStringRecordWriter() {
        List<TestRecord> testRecords = Arrays.asList(
                new TestRecord("key1", 1, null),
                new TestRecord("key1", 1, new TestRecord.TestSubRecord("aaa")),
                new TestRecord("key2", 2, new TestRecord.TestSubRecord("aaa"))
        );

        try {
            String json = JSON_MAPPER.get().writeValueAsString(testRecords);
            sendTypedMessages(jsonStringTopicName.toString(), SchemaType.STRING,
                    Arrays.asList(json, json), Optional.empty(), String.class);

            Consumer<Message<GenericRecord>> handle = getJsonByteAndStringHandler(jsonStringTopicName);
            consumerMessages(jsonStringTopicName.toString(), Schema.AUTO_CONSUME(), handle, 2, 2000);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private Consumer<Message<GenericRecord>> getJsonByteAndStringHandler(TopicName topic) {
        return msg -> {
            try {
                Schema<GenericRecord> schema = (Schema<GenericRecord>) msg.getReaderSchema().get();
                initSchema(schema);
                Map<String, Object> message = getJSONMessage(topic, msg);
                Assert.assertEquals(message.size(), 2);
                List<Map<String, Object>> value = (List<Map<String, Object>>) message.get("value");
                Assert.assertEquals(value.size(), 3);
            } catch (Exception e) {
                log.error("formatter handle message is fail", e);
                fail();
            }
        };
    }

    @Override
    public Format<GenericRecord> getFormat() {
        return format;
    }

    @Override
    public String expectedFormatExtension() {
        return ".json";
    }

    @Override
    protected boolean supportMetadata() {
        return true;
    }

    public org.apache.avro.generic.GenericRecord getFormatGeneratedRecord(TopicName topicName,
                                                                          Message<GenericRecord> msg) throws Exception {
        return null;
    }

    @Override
    public DynamicMessage getDynamicMessage(TopicName topicName, Message<GenericRecord> msg) throws Exception {
        return null;
    }

    @Override
    public Consumer<Message<GenericRecord>> getMessageConsumer(TopicName topic) {
        return msg -> {
            try {
                Schema<GenericRecord> schema = (Schema<GenericRecord>) msg.getReaderSchema().get();
                initSchema(schema);
                Map<String, Object> formatGeneratedRecord = getJSONMessage(topic, msg);
                assertEquals(msg.getValue(), formatGeneratedRecord);
            } catch (Exception e) {
                log.error("formatter handle message is fail", e);
                fail();
            }
        };
    }

    @Override
    public Consumer<Message<GenericRecord>> getJSONMessageConsumer(TopicName topic) {
        return msg -> {
            try {
                Schema<GenericRecord> schema = (Schema<GenericRecord>) msg.getReaderSchema().get();
                initSchema(schema);
                Map<String, Object> message = getJSONMessage(topic, msg);
                assertEquals(msg.getValue(), message);
            } catch (Exception e) {
                log.error("formatter handle message is fail", e);
                fail();
            }
        };
    }

    @Override
    public Map<String, Object> getJSONMessage(TopicName topicName, Message<GenericRecord> msg) throws Exception {
        Schema<GenericRecord> schema = (Schema<GenericRecord>) msg.getReaderSchema().get();
        Record<GenericRecord> mockRecord = mock(Record.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topicName.toString()));
        when(mockRecord.getPartitionIndex()).thenReturn(Optional.of(0));
        when(mockRecord.getMessage()).thenReturn(Optional.of(msg));
        when(mockRecord.getValue()).thenReturn(msg.getValue());
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topicName, 0)));
        when(mockRecord.getRecordSequence()).thenReturn(Optional.of(3221225506L));
        when(mockRecord.getSchema()).thenReturn(schema);

        List<Record<GenericRecord>> records = new ArrayList<>();
        records.add(mockRecord);

        ByteSource byteSource = getFormat().recordWriter(records.listIterator());
        Map<String, Object> record = JSON_MAPPER.get().readValue(byteSource.read(), Map.class);
        return record;
    }

    private void assertEquals(GenericRecord msgValue, Map<String, Object> record) {
        List<Field> fields = msgValue.getFields();
        switch (msgValue.getSchemaType()) {
            case PROTOBUF_NATIVE:
                assertEquals((DynamicMessage) msgValue.getNativeObject(), record);
                break;
            default: {
                for (String fieldName : record.keySet()) {
                    Field genericField = fields.stream().filter(field ->
                            field.getName().equals(fieldName)).findFirst().orElse(null);
                    if (genericField != null) {
                        Object value = msgValue.getField(genericField);
                        if (!(value instanceof GenericRecord)) {
                            Assert.assertEquals(record.get(fieldName), value);
                        }
                    }
                }
                break;
            }
        }
    }

    @Override
    public Consumer<Message<GenericRecord>> getProtobufNativeMessageConsumer(TopicName topic) {
        return msg -> {
            try {
                Schema<GenericRecord> schema = (Schema<GenericRecord>) msg.getReaderSchema().get();
                initSchema(schema);
                Map<String, Object> message = getJSONMessage(topic, msg);
                assertEquals(msg.getValue(), message);
            } catch (Exception e) {
                log.error("formatter handle message is fail", e);
                fail();
            }
        };
    }
}
