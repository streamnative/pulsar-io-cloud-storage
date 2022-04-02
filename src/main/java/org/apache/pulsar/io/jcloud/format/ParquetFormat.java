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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.BytesOutputStream;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;
import org.apache.pulsar.io.jcloud.util.MetadataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * parquet format.
 */
public class ParquetFormat implements Format<GenericRecord>, InitConfiguration<BlobStoreAbstractConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFormat.class);

    private Schema rootAvroSchema;
    private org.apache.pulsar.client.api.Schema<GenericRecord> internalSchema;

    private boolean useMetadata;
    private boolean useHumanReadableMessageId;
    private boolean useHumanReadableSchemaVersion;

    @Override
    public String getExtension() {
        return ".parquet";
    }

    @Override
    public void configure(BlobStoreAbstractConfig configuration) {
        this.useMetadata = configuration.isWithMetadata();
        this.useHumanReadableMessageId = configuration.isUseHumanReadableMessageId();
        this.useHumanReadableSchemaVersion = configuration.isUseHumanReadableSchemaVersion();
    }

    @Override
    public void initSchema(org.apache.pulsar.client.api.Schema<GenericRecord> schema) {
        internalSchema = schema;
        rootAvroSchema = AvroRecordUtil.convertToAvroSchema(schema);
        if (useMetadata){
            rootAvroSchema = MetadataUtil.setMetadataSchema(rootAvroSchema,
                    useHumanReadableMessageId, useHumanReadableSchemaVersion);
        }

        LOGGER.debug("Using avro schema: {}", rootAvroSchema);
    }

    @Override
    public ByteBuffer recordWriterBuf(Iterator<Record<GenericRecord>> records) throws Exception {
        int pageSize = 64 * 1024;
        ParquetWriter<Object> parquetWriter = null;
        S3ParquetOutputFile file = new S3ParquetOutputFile();
        try {
            parquetWriter = AvroParquetWriter
                    .builder(file)
                    .withPageSize(pageSize)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withCompressionCodec(CompressionCodecName.GZIP)
                    .withSchema(rootAvroSchema).build();

            while (records.hasNext()) {
                final Record<GenericRecord> next = records.next();
                GenericRecord genericRecord = next.getValue();
                if (genericRecord.getSchemaType() == SchemaType.BYTES
                        && internalSchema.getSchemaInfo().getType() == SchemaType.PROTOBUF_NATIVE) {
                    genericRecord = internalSchema.decode((byte[]) next.getValue().getNativeObject());
                }
                org.apache.avro.generic.GenericRecord writeRecord = AvroRecordUtil
                        .convertGenericRecord(genericRecord, rootAvroSchema);
                if (useMetadata) {
                    org.apache.avro.generic.GenericRecord metadataRecord =
                            MetadataUtil.extractedMetadataRecord(next,
                                    useHumanReadableMessageId, useHumanReadableSchemaVersion);
                    writeRecord.put(MetadataUtil.MESSAGE_METADATA_KEY, metadataRecord);
                }
                parquetWriter.write(writeRecord);
            }
        } finally {
            IOUtils.closeQuietly(parquetWriter);
        }
        return ByteBuffer.wrap(file.toByteArray());
    }

    private static class S3ParquetOutputFile implements OutputFile {
        private static final int DEFAULT_BLOCK_SIZE = 0;

        private BytesOutputStream s3out;

        S3ParquetOutputFile() {
        }

        @Override
        public PositionOutputStream create(long l) throws IOException {
            s3out = new BytesOutputStream();
            return s3out;
        }

        @Override
        public PositionOutputStream createOrOverwrite(long l) throws IOException {
            s3out = new BytesOutputStream();
            return s3out;
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return DEFAULT_BLOCK_SIZE;
        }

        private byte[] toByteArray() {
            if (s3out == null) {
                return null;
            }
            return s3out.getByteArrayOutputStream().toByteArray();
        }
    }
}
