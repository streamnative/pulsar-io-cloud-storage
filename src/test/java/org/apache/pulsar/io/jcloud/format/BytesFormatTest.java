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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * avro format test.
 */
public class BytesFormatTest extends FormatTestBase {
    private static final Logger log = LoggerFactory.getLogger(BytesFormatTest.class);
    private BytesFormat format = new BytesFormat();

    @Override
    public Format<GenericRecord> getFormat() {
        return format;
    }

    @Override
    public String expectedFormatExtension() {
        return ".raw";
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
            final Schema<GenericRecord> schema = AvroRecordUtil.extractPulsarSchema(msg);
            format.initSchema(schema);
            format.configure(null);
            ByteSource byteSource = getFormat().recordWriter(records.listIterator());


            final byte[] expecteds =
                    ArrayUtils.addAll(msg.getData(), System.lineSeparator().getBytes(StandardCharsets.UTF_8));

            Assert.assertArrayEquals(expecteds, byteSource.read());
        } catch (Exception e) {
            log.error("", e);
            Assert.fail();
        }
    }
}