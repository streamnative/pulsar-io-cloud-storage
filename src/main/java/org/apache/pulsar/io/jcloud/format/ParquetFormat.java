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
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.BytesOutputStream;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;
import org.apache.pulsar.io.jcloud.util.MetadataUtil;
import org.apache.pulsar.jcloud.shade.com.google.common.io.ByteSource;


/**
 * parquet format.
 */
public class ParquetFormat implements Format<GenericRecord>, InitConfiguration<BlobStoreAbstractConfig> {
    @Override
    public String getExtension() {
        return ".parquet";
    }

    private Schema rootAvroSchema;

    private boolean useMetadata;

    @Override
    public void configure(BlobStoreAbstractConfig configuration) {
        this.useMetadata = configuration.isWithMetadata();
    }

    @Override
    public void initSchema(org.apache.pulsar.client.api.Schema<GenericRecord> schema) {
        rootAvroSchema = AvroRecordUtil.convertToAvroSchema(schema);
        if (useMetadata){
            rootAvroSchema = MetadataUtil.setMetadataSchema(rootAvroSchema);
        }
    }

    @Override
    public ByteSource recordWriter(Iterator<Record<GenericRecord>> records) throws Exception {
        int pageSize = 64 * 1024;
        ParquetWriter<Object> parquetWriter = null;
        S3ParquetOutputFile file = new S3ParquetOutputFile();
        try {
            parquetWriter = AvroParquetWriter
                    .builder(file)
                    .withPageSize(pageSize)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withCompressionCodec(CompressionCodecName.GZIP)
                    .withSchema(rootAvroSchema)
                    .build();

            while (records.hasNext()) {
                final Record<GenericRecord> next = records.next();
                org.apache.avro.generic.GenericRecord writeRecord = AvroRecordUtil
                        .convertGenericRecord(next.getValue(), rootAvroSchema);
                if (useMetadata) {
                    org.apache.avro.generic.GenericRecord metadataRecord = MetadataUtil.extractedMetadataRecord(next);
                    writeRecord.put(MetadataUtil.MESSAGE_METADATA_KEY, metadataRecord);
                }
                parquetWriter.write(writeRecord);
            }
        } finally {
            IOUtils.closeQuietly(parquetWriter);
        }
        return ByteSource.wrap(file.toByteArray());
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
