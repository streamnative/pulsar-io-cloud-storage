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
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;
import org.apache.pulsar.io.jcloud.util.MetadataUtil;
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

    public void handleMessage(TopicName topicName, Message<GenericRecord> msg) {
        @SuppressWarnings("unchecked")
        PulsarRecord<GenericRecord> test = PulsarRecord.<GenericRecord>builder()
                .topicName(topicName.toString())
                .partition(0)
                .message(msg)
                .build();
        List<Record<GenericRecord>> records = new ArrayList<>();
        records.add(new SinkRecord<>(test, test.getValue()));
        try {
            final Schema<GenericRecord> schema = (Schema<GenericRecord>) msg.getReaderSchema().get();
            final BlobStoreAbstractConfig config = new BlobStoreAbstractConfig();
            config.setUseMetadata(true);
            ((InitConfiguration<BlobStoreAbstractConfig>) getFormat()).configure(config);
            getFormat().initSchema(schema);
            ByteSource byteSource = getFormat().recordWriter(records.listIterator());


            final org.apache.avro.Schema avroSchema = AvroRecordUtil.convertToAvroSchema(schema);
            final GenericDatumReader<Object> datumReader =
                    new GenericDatumReader<>(MetadataUtil.setMetadataSchema(avroSchema));
            final SeekableByteArrayInput input = new SeekableByteArrayInput(byteSource.read());
            final DataFileReader<Object> objects = new DataFileReader<>(input, datumReader);

            final org.apache.avro.generic.GenericRecord genericRecord =
                    (org.apache.avro.generic.GenericRecord) objects.next();
            assertEquals(msg.getValue(), genericRecord);
        } catch (Exception e) {
            log.error("", e);
            Assert.fail();
        }
    }
}