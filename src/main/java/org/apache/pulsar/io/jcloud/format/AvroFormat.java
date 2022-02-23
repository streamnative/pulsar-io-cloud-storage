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

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;
import org.apache.pulsar.io.jcloud.util.MetadataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * avro format.
 */
public class AvroFormat implements Format<GenericRecord> , InitConfiguration<BlobStoreAbstractConfig>{

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroFormat.class);

    private final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());

    private Schema rootAvroSchema;

    private boolean useMetadata;
    private boolean useHumanReadableMessageId;
    private boolean useHumanReadableSchemaVersion;
    private CodecFactory codecFactory;

    @Override
    public String getExtension() {
        return ".avro";
    }

    @Override
    public void configure(BlobStoreAbstractConfig configuration) {
        this.useMetadata = configuration.isWithMetadata();
        this.useHumanReadableMessageId = configuration.isUseHumanReadableMessageId();
        this.useHumanReadableSchemaVersion = configuration.isUseHumanReadableSchemaVersion();
        String codecName = configuration.getAvroCodec();
        if (codecName == null) {
            this.codecFactory = CodecFactory.nullCodec();
        } else {
            try {
                this.codecFactory = CodecFactory.fromString(codecName);
                LOGGER.info("Use AVRO codec: {}", codecName);
            } catch (Throwable cause) {
                LOGGER.warn("Failed to initialize the codec factory", cause);
                this.codecFactory = CodecFactory.nullCodec();
                LOGGER.info("Fallback to use null codec");
            }
        }
    }

    @Override
    public void initSchema(org.apache.pulsar.client.api.Schema<GenericRecord> schema) {
        rootAvroSchema = AvroRecordUtil.convertToAvroSchema(schema);
        if (useMetadata){
            rootAvroSchema = MetadataUtil.setMetadataSchema(rootAvroSchema,
                    useHumanReadableMessageId, useHumanReadableSchemaVersion);
        }
    }

    @Override
    public ByteBuffer recordWriterBuf(Iterator<Record<GenericRecord>> records) throws Exception {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        writer.setCodec(codecFactory);
        try (DataFileWriter<Object> fileWriter = writer.create(rootAvroSchema, byteArrayOutputStream)) {
            while (records.hasNext()) {
                final Record<GenericRecord> next = records.next();
                org.apache.avro.generic.GenericRecord writeRecord = AvroRecordUtil
                        .convertGenericRecord(next.getValue(), rootAvroSchema);

                if (useMetadata) {
                    org.apache.avro.generic.GenericRecord metadataRecord =
                            MetadataUtil.extractedMetadataRecord(next,
                                    useHumanReadableMessageId, useHumanReadableSchemaVersion);
                    writeRecord.put(MetadataUtil.MESSAGE_METADATA_KEY, metadataRecord);
                }
                fileWriter.append(writeRecord);
            }
            fileWriter.flush();
        }
        return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    }

    @VisibleForTesting
    public Schema getRootAvroSchema() {
        return rootAvroSchema;
    }
}
