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
package org.apache.pulsar.io.jcloud.format.parquet;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

/**
 * ParquetWriter for protobuf messages with descriptor.
 */
public class ProtobufParquetWriter<T extends MessageOrBuilder> extends ParquetWriter<T> {
    @Deprecated
    public ProtobufParquetWriter(Path file, Class<? extends Message> protoMessage,
                                 CompressionCodecName compressionCodecName, int blockSize,
                                 int pageSize) throws IOException {
        super(file, new ProtobufDescriptorWriteSupport(protoMessage),
                compressionCodecName, blockSize, pageSize);
    }

    @Deprecated
    public ProtobufParquetWriter(Path file, Class<? extends Message> protoMessage,
                                 CompressionCodecName compressionCodecName, int blockSize,
                                 int pageSize, boolean enableDictionary, boolean validating) throws IOException {
        super(file, new ProtobufDescriptorWriteSupport(protoMessage),
                compressionCodecName, blockSize, pageSize, enableDictionary, validating);
    }

    @Deprecated
    public ProtobufParquetWriter(Path file, Class<? extends Message> protoMessage) throws IOException {
        this(file, protoMessage, CompressionCodecName.UNCOMPRESSED,
                DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
    }

    public static <T> Builder<T> builder(Path file) {
        return new Builder<T>(file);
    }

    public static <T> Builder<T> builder(OutputFile file) {
        return new Builder<T>(file);
    }

    private static <T extends MessageOrBuilder> WriteSupport<T> writeSupport(Descriptors.Descriptor descriptor) {
        return new ProtobufDescriptorWriteSupport<>(descriptor);
    }

    /**
     * Builder for ProtobufParquetWriter.
     */
    public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {
        Descriptors.Descriptor descriptor = null;

        private Builder(Path file) {
            super(file);
        }

        private Builder(OutputFile file) {
            super(file);
        }

        protected Builder<T> self() {
            return this;
        }

        public Builder<T> withDescriptor(Descriptors.Descriptor descriptor) {
            this.descriptor = descriptor;
            return this;
        }

        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return (WriteSupport<T>) ProtobufParquetWriter.writeSupport(descriptor);
        }
    }
}
