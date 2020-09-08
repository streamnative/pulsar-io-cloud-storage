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
package org.apache.pulsar.ecosystem.io.s3.format;

import com.google.common.io.ByteSource;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
import org.apache.pulsar.ecosystem.io.s3.BlobStoreAbstractConfig;
import org.apache.pulsar.ecosystem.io.s3.util.AvroRecordUtil;
import org.apache.pulsar.functions.api.Record;

/**
 * avro format.
 * @param <V> config
 */
public class AvroFormat<V extends BlobStoreAbstractConfig> implements Format<V, Record<GenericRecord>> {

    @Override
    public String getExtension() {
        return ".avro";
    }

    @Override
    public ByteSource recordWriter(V config, Record<GenericRecord> record) throws Exception {

        MessageImpl<GenericRecord> message = getMessage(record);
        Schema rootAvroSchema = AvroRecordUtil.getAvroSchema(message.getSchema());
        org.apache.avro.generic.GenericRecord writeRecord = AvroRecordUtil
                .convertGenericRecord(record.getValue(), rootAvroSchema);

        AvroWriter<org.apache.avro.generic.GenericRecord> avroWriter = new AvroWriter<>(rootAvroSchema, true);
        return ByteSource.wrap(avroWriter.write(writeRecord));
    }

    private MessageImpl<GenericRecord> getMessage(Record<GenericRecord> record) {
        Message<GenericRecord> message = record.getMessage().get();
        if (message instanceof TopicMessageImpl){
            message = ((TopicMessageImpl<GenericRecord>) message).getMessage();
        }
        return (MessageImpl<GenericRecord>) message;
    }
}
