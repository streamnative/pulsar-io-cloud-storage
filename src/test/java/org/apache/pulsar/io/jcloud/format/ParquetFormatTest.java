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

import com.google.common.io.ByteSource;

import java.io.ByteArrayOutputStream;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.avro.util.Utf8;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.jcloud.PulsarTestBase;
import org.apache.pulsar.io.jcloud.bo.TestRecord;
import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;
import org.apache.pulsar.io.jcloud.support.ParquetInputFile;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.Sets;

/**
 * parquet format test.
 */
public class ParquetFormatTest extends PulsarTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFormatTest.class);

    private ParquetFormat<CloudStorageSinkConfig> parquetFormat = new ParquetFormat<>();

    private TopicName avroTopicName = TopicName.get("test-parquet-avro");
    private TopicName jsonTopicName = TopicName.get("test-parquet-json");
//    @Before
    public void setUp() throws Exception {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getAdminUrl())
                .build();
        pulsarAdmin.namespaces().createNamespace("public/default", Sets.newHashSet("standalone"));
        pulsarAdmin.topics().createPartitionedTopic(avroTopicName.toString(), 1);
        pulsarAdmin.topics().createSubscription(avroTopicName.toString(), "test", MessageId.earliest);
    }

    @Test
    public void testGetExtension() {
       Assert.assertEquals(".parquet", parquetFormat.getExtension());
    }

    @Test
    @Ignore("unable to start admin client")
    public void testRecordWriter() throws Exception {

        serviceUrl = "pulsar://localhost:6650";

        List<TestRecord> testRecords = Arrays.asList(
                new TestRecord("key1", 1, null),
                new TestRecord("key1", 1, new TestRecord.TestSubRecord("aaa"))
        );

//        sendTypedMessages(topicName.toString(), SchemaType.AVRO, testRecords, Optional.empty(), TestRecord.class);


        Consumer<Message<GenericRecord>> handle = msg -> {
            handleMessage(jsonTopicName, msg);
        };
        consumerMessages(jsonTopicName.toString(), Schema.AUTO_CONSUME(), handle, testRecords.size());
    }

    private void handleMessage(TopicName topicName, Message<GenericRecord> msg)  {
        @SuppressWarnings("unchecked")
        PulsarRecord<GenericRecord> test = PulsarRecord.<GenericRecord>builder()
                .topicName(topicName.toString())
                .partition(0)
                .message(msg)
                .build();
        try {
            ByteSource byteSource = parquetFormat.recordWriter(null, test);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            stream.write(byteSource.read());
            ParquetInputFile file = new ParquetInputFile("a.parquet", stream);


            ParquetReader<org.apache.avro.generic.GenericRecord> reader = AvroParquetReader
                    .<org.apache.avro.generic.GenericRecord>builder(file)
                    .build();

            org.apache.avro.generic.GenericRecord record = reader.read();

            assertEquals(msg.getValue(), record);
            System.out.println("build.record() = " + record);
            System.out.println("record.getClass() = " + record.getClass());
        } catch (Exception e) {
            LOGGER.error("", e);
            Assert.fail();
        }
    }

    private void assertEquals(GenericRecord msgValue, org.apache.avro.generic.GenericRecord record) {
        for (Field field : msgValue.getFields()) {
            Object sourceValue = getField(msgValue, field);
            Object newValue = record.get(field.getName());
            if (newValue instanceof Utf8){
                newValue = ((Utf8) newValue).toString();
            }
            if (sourceValue instanceof GenericRecord && newValue instanceof org.apache.avro.generic.GenericRecord){
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
}