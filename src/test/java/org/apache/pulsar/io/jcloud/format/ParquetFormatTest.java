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
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.jcloud.support.ParquetInputFile;
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
        return true;
    }

    public org.apache.avro.generic.GenericRecord getFormatGeneratedRecord(TopicName topicName,
                                                                          Message<GenericRecord> msg) throws Exception {
        @SuppressWarnings("unchecked")
        PulsarRecord<GenericRecord> test = PulsarRecord.<GenericRecord>builder()
                .topicName(topicName.toString())
                .partition(0)
                .message(msg)
                .build();
        List<Record<GenericRecord>> records = new ArrayList<>();
        records.add(new SinkRecord<>(test, test.getValue()));

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
}