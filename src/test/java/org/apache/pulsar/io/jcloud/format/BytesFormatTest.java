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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.io.ByteSource;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * avro format test.
 */
public class BytesFormatTest extends FormatTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(BytesFormatTest.class);

    private BytesFormat format = new BytesFormat();

    @Override
    public Format<GenericRecord> getFormat() {
        return format;
    }

    @Override
    public String expectedFormatExtension() {
        return ".raw";
    }

    @Override
    protected boolean supportMetadata() {
        return false;
    }

    public org.apache.avro.generic.GenericRecord getFormatGeneratedRecord(TopicName topicName,
                                                                          Message<GenericRecord> msg) throws Exception {
        Record<GenericRecord> mockRecord = mock(Record.class);
        Schema<GenericRecord> mockSchema = mock(Schema.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topicName.toString()));
        when(mockRecord.getPartitionIndex()).thenReturn(Optional.of(0));
        when(mockRecord.getMessage()).thenReturn(Optional.of(msg));
        when(mockRecord.getValue()).thenReturn(msg.getValue());
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topicName, 0)));
        when(mockRecord.getRecordSequence()).thenReturn(Optional.of(3221225506L));
        when(mockRecord.getSchema()).thenReturn(mockSchema);

        List<Record<GenericRecord>> records = new ArrayList<>();
        records.add(mockRecord);

        ByteSource byteSource = getFormat().recordWriter(records.listIterator());
        final byte[] expecteds =
                ArrayUtils.addAll(msg.getData(), System.lineSeparator().getBytes(StandardCharsets.UTF_8));
        Assert.assertArrayEquals(expecteds, byteSource.read());
        if (msg.getValue().getSchemaType().isPrimitive()){
            return null;
        }
        return AvroRecordUtil.convertGenericRecord(
                mockRecord.getValue(),
                AvroRecordUtil.convertToAvroSchema(msg.getReaderSchema().get()));
    }

    @Test
    public void testStringWriter() throws Exception {

        TopicName topic = TopicName.get("test-string" + RandomStringUtils.random(5));

        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getAdminUrl())
                .build();
        pulsarAdmin.topics().createPartitionedTopic(topic.toString(), 1);
        pulsarAdmin.topics().createSubscription(topic.toString(), "test", MessageId.earliest);
        pulsarAdmin.close();
        List<String> testRecords = Arrays.asList("key1", "key2");

        sendTypedMessages(topic.toString(), SchemaType.STRING, testRecords, Optional.empty(), String.class);

        Consumer<Message<GenericRecord>>
                handle = msg -> {
            try {
                initSchema((Schema<GenericRecord>) msg.getReaderSchema().get());
                getFormatGeneratedRecord(topic, msg);
            } catch (Exception e) {
                LOGGER.error("formatter handle message is fail", e);
                Assert.fail();
            }
        };
        consumerMessages(topic.toString(), Schema.AUTO_CONSUME(), handle, testRecords.size(), 2000);
    }
}
