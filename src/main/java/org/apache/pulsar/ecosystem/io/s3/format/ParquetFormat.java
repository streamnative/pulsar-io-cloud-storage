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
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.s3.S3OutputStream;
import org.apache.pulsar.ecosystem.io.s3.util.AvroRecordUtil;
import org.apache.pulsar.functions.api.Record;


/**
 * parquet format.
 */
public class ParquetFormat<V> implements Format<V, Record<GenericRecord>>{
    @Override
    public String getExtension() {
        return ".parquet";
    }

    @Override
    public ByteSource recordWriter(V config, Record<GenericRecord> record) throws Exception {
        Schema rootAvroSchema = AvroRecordUtil.getAvroSchema(record.getSchema());
        org.apache.avro.generic.GenericRecord writeRecord = AvroRecordUtil
                .convertGenericRecord(record.getValue(), rootAvroSchema);

        int pageSize = 64 * 1024;

        ParquetWriter<Object> parquetWriter = null;
        S3ParquetOutputFile file = new S3ParquetOutputFile();
        try {
            parquetWriter = AvroParquetWriter
                    .builder(file)
                    .withPageSize(pageSize)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withSchema(rootAvroSchema)
                    .build();
            parquetWriter.write(writeRecord);
        } finally {
            IOUtils.closeQuietly(parquetWriter);
        }
        return ByteSource.wrap(file.toByteArray());
    }

    private static class S3ParquetOutputFile implements OutputFile {
        private static final int DEFAULT_BLOCK_SIZE = 0;

        private S3OutputStream s3out;

        S3ParquetOutputFile() {

        }

        @Override
        public PositionOutputStream create(long l) throws IOException {
            s3out = new S3OutputStream();
            return s3out;
        }

        @Override
        public PositionOutputStream createOrOverwrite(long l) throws IOException {
            s3out = new S3OutputStream();
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

        private byte[] toByteArray(){
            if (s3out == null){
                return null;
            }
            return s3out.getByteArrayOutputStream().toByteArray();
        }
    }
}
