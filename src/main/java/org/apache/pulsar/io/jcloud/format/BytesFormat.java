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
import java.util.Iterator;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.util.HexStringUtils;
import org.apache.pulsar.jcloud.shade.com.google.common.io.ByteArrayDataOutput;
import org.apache.pulsar.jcloud.shade.com.google.common.io.ByteStreams;

/**
 * bytes format.
 */
public class BytesFormat implements Format<GenericRecord>, InitConfiguration<BlobStoreAbstractConfig> {

    private byte[] lineSeparatorBytes;

    @Override
    public void configure(BlobStoreAbstractConfig configuration) {
        lineSeparatorBytes = HexStringUtils.convertHexStringToBytes(configuration.getBytesFormatTypeSeparator());
    }

    @Override
    public String getExtension() {
        return ".raw";
    }

    @Override
    public void initSchema(Schema<GenericRecord> schema) {
    }

    @Override
    public ByteBuffer recordWriterBuf(Iterator<Record<GenericRecord>> record) throws Exception {
        final ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
        while (record.hasNext()) {
            final Record<GenericRecord> next = record.next();
            final Message<GenericRecord> message = next.getMessage().get();
            final byte[] data = message.getData();
            dataOutput.write(data);
            dataOutput.write(lineSeparatorBytes);
        }
        return ByteBuffer.wrap(dataOutput.toByteArray());
    }
}
