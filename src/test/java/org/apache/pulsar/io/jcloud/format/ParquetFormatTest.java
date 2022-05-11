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
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.schema.proto.Test;
import org.apache.pulsar.io.jcloud.support.ParquetInputFile;
import org.apache.pulsar.jcloud.shade.com.google.common.io.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * parquet format test.
 */
public class ParquetFormatTest extends FormatTestBase {

    private static final Logger log = LoggerFactory.getLogger(ParquetFormatTest.class);

    private ParquetFormat parquetFormat = new ParquetFormat();


    @Override
    public Format<GenericRecord> getFormat() {
        return parquetFormat;
    }

    @Override
    public String expectedFormatExtension() {
        return ".parquet";
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
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topicName, 0)));
        when(mockRecord.getRecordSequence()).thenReturn(Optional.of(3221225506L));
        when(mockRecord.getSchema()).thenReturn(mockSchema);
        when(mockRecord.getValue()).thenReturn(msg.getValue());

        List<Record<GenericRecord>> records = new ArrayList<>();
        records.add(mockRecord);

        ByteSource byteSource = getFormat().recordWriter(records.listIterator());
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(byteSource.read());
        ParquetInputFile file = new ParquetInputFile("tmp.parquet", stream);

        ParquetReader<org.apache.avro.generic.GenericRecord> reader = AvroParquetReader
                .<org.apache.avro.generic.GenericRecord>builder(file)
                .withDataModel(GenericData.get())
                .build();
        org.apache.avro.generic.GenericRecord record = reader.read();

        return record;

    }

    @Override
    public DynamicMessage getDynamicMessage(TopicName topicName, Message<GenericRecord> msg) throws Exception {
        Record<GenericRecord> mockRecord = mock(Record.class);
        Schema<GenericRecord> mockSchema = mock(Schema.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topicName.toString()));
        when(mockRecord.getPartitionIndex()).thenReturn(Optional.of(0));
        when(mockRecord.getMessage()).thenReturn(Optional.of(msg));
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topicName, 0)));
        when(mockRecord.getRecordSequence()).thenReturn(Optional.of(3221225506L));
        when(mockRecord.getSchema()).thenReturn(mockSchema);
        when(mockRecord.getValue()).thenReturn(msg.getValue());

        List<Record<GenericRecord>> records = new ArrayList<>();
        records.add(mockRecord);

        ByteSource byteSource = getFormat().recordWriter(records.listIterator());
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(byteSource.read());
        ParquetInputFile file = new ParquetInputFile("tmp.parquet", stream);

        Configuration configuration = new Configuration();
        configuration.set("parquet.proto.class", Test.TestMessage.class.getName());

        ParquetReader<Test.TestMessage.Builder> reader =
                ProtoParquetReader.<Test.TestMessage.Builder>builder(file).withConf(configuration).build();

        Test.TestMessage.Builder record = reader.read();

        return DynamicMessage.newBuilder(record.build()).build();
    }

    @Override
    public Map<String, Object> getJSONMessage(TopicName topicName, Message<GenericRecord> msg) throws Exception {
        return null;
    }
}