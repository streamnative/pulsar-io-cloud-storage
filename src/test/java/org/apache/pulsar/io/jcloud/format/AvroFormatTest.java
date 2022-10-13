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
import com.google.protobuf.DynamicMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;
import org.apache.pulsar.io.jcloud.util.MetadataUtil;
import org.apache.pulsar.jcloud.shade.com.google.common.io.ByteSource;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * avro format test.
 */
public class AvroFormatTest extends FormatTestBase {
    private static final Logger log = LoggerFactory.getLogger(AvroFormatTest.class);
    private AvroFormat format = new AvroFormat();

    @Override
    public Format<GenericRecord> getFormat() {
        return format;
    }

    @Override
    public String expectedFormatExtension() {
        return ".avro";
    }

    @Override
    protected boolean supportMetadata() {
        return true;
    }

    public org.apache.avro.generic.GenericRecord getFormatGeneratedRecord(TopicName topicName,
                                                                          Message<GenericRecord> msg) throws Exception {
        final Schema<GenericRecord> schema = (Schema<GenericRecord>) msg.getReaderSchema().get();
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


        org.apache.avro.Schema avroSchema = AvroRecordUtil.convertToAvroSchema(schema);
        avroSchema = MetadataUtil.setMetadataSchema(avroSchema);

        final GenericDatumReader<Object> datumReader =
                new GenericDatumReader<>(avroSchema);
        ByteSource byteSource = getFormat().recordWriter(records.listIterator());
        final SeekableByteArrayInput input = new SeekableByteArrayInput(byteSource.read());
        final DataFileReader<Object> objects = new DataFileReader<>(input, datumReader);
        return (org.apache.avro.generic.GenericRecord) objects.next();
    }

    @Override
    public DynamicMessage getDynamicMessage(TopicName topicName, Message<GenericRecord> msg) throws Exception {
        return null;
    }

    @Override
    public Map<String, Object> getJSONMessage(TopicName topicName, Message<GenericRecord> msg) throws Exception {
        return null;
    }

    @Override
    public Consumer<Message<GenericRecord>> getProtobufNativeMessageConsumer(TopicName topic) {
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
                log.error("formatter handle message is fail", e);
                Assert.fail();
            }
        };
    }
}