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

import static org.apache.pulsar.io.jcloud.util.MetadataUtil.MESSAGE_METADATA_KEY;
import static org.apache.pulsar.io.jcloud.util.MetadataUtil.getMetadataFromMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.protocol.schema.ProtobufNativeSchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.format.parquet.ProtobufParquetWriter;
import org.apache.pulsar.io.jcloud.format.parquet.proto.Metadata;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;
import org.apache.pulsar.io.jcloud.util.BytesOutputStream;
import org.apache.pulsar.io.jcloud.util.MetadataUtil;

/**
 * parquet format.
 */
@Slf4j
public class ParquetFormat implements Format<GenericRecord>, InitConfiguration<BlobStoreAbstractConfig> {
    private Schema rootAvroSchema = null;
    private Descriptors.Descriptor descriptor = null;
    private org.apache.pulsar.client.api.Schema<GenericRecord> internalSchema;

    private boolean useMetadata;
    private boolean useHumanReadableMessageId;
    private boolean useHumanReadableSchemaVersion;
    private boolean includeTopicToMetadata;
    private boolean includePublishTimeToMetadata;
    private boolean includeMessageKeyToMetadata;

    private CompressionCodecName compressionCodecName = CompressionCodecName.GZIP;

    @Override
    public String getExtension() {
        return ".parquet";
    }

    @Override
    public void configure(BlobStoreAbstractConfig configuration) {
        this.useMetadata = configuration.isWithMetadata();
        this.useHumanReadableMessageId = configuration.isUseHumanReadableMessageId();
        this.useHumanReadableSchemaVersion = configuration.isUseHumanReadableSchemaVersion();
        this.includeTopicToMetadata = configuration.isIncludeTopicToMetadata();
        this.includePublishTimeToMetadata = configuration.isIncludePublishTimeToMetadata();
        this.includeMessageKeyToMetadata = configuration.isIncludeMessageKeyToMetadata();
        this.compressionCodecName = CompressionCodecName.fromConf(configuration.getParquetCodec());
    }

    @Override
    public void initSchema(org.apache.pulsar.client.api.Schema<GenericRecord> schema) {
        if (!schema.equals(internalSchema)) {
            internalSchema = schema;
            if (internalSchema.getSchemaInfo().getType().isPrimitive()) {
                throw new UnsupportedOperationException(
                        "Parquet format do not support primitive record (schemaType=" + internalSchema.getSchemaInfo()
                                .getType() + ")");
            }
            if (internalSchema.getSchemaInfo().getType() == SchemaType.PROTOBUF_NATIVE) {
                if (useMetadata) {
                    // Very hacky way to append metadata schema into protobuf message's descriptor.
                    try {
                        ProtobufNativeSchemaData schemaData =
                                new ObjectMapper().readValue(internalSchema.getSchemaInfo().getSchema(),
                                ProtobufNativeSchemaData.class);

                        // Get the descriptor from the pulsar schema.
                        Map<String, DescriptorProtos.FileDescriptorProto> fileDescriptorProtoCache = new HashMap<>();
                        Map<String, Descriptors.FileDescriptor> fileDescriptorCache = new HashMap<>();
                        DescriptorProtos.FileDescriptorSet fileDescriptorSet =
                                DescriptorProtos.FileDescriptorSet.parseFrom(schemaData.getFileDescriptorSet());
                        fileDescriptorSet.getFileList().forEach(fileDescriptorProto ->
                                fileDescriptorProtoCache.put(fileDescriptorProto.getName(), fileDescriptorProto));

                        // rootFileDescriptorProto is the targeting file descriptor.
                        DescriptorProtos.FileDescriptorProto rootFileDescriptorProto =
                                fileDescriptorProtoCache.get(schemaData.getRootFileDescriptorName());

                        // get metadata descriptor.
                        Descriptors.Descriptor metadataDescriptor =
                                Metadata.PulsarIOCSCProtobufMessageMetadata.getDescriptor();

                        // put the metadata ile descriptor into the file descriptor cache
                        // so it can be used in the next step.
                        fileDescriptorCache.put(metadataDescriptor.getFile().getName(), metadataDescriptor.getFile());

                        // get the root descriptor builder
                        DescriptorProtos.FileDescriptorProto.Builder rootFileDescriptorProtoBuilder =
                                rootFileDescriptorProto.toBuilder();

                        // add the metadata descriptor to the root file descriptor.
                        rootFileDescriptorProtoBuilder.addDependency(metadataDescriptor.getFile().getName());

                        String[] paths = StringUtils.removeFirst(schemaData.getRootMessageTypeName(),
                                        rootFileDescriptorProtoBuilder.getPackage())
                                .replaceFirst("\\.", "").split("\\.");

                        //extract root message
                        final String[] finalPaths = paths;
                        DescriptorProtos.DescriptorProto.Builder descriptorBuilder = rootFileDescriptorProtoBuilder
                                .getMessageTypeBuilderList().stream()
                                .filter(descriptorProto ->
                                        descriptorProto.getName().equals(finalPaths[0])).findFirst()
                                .orElseThrow(() -> new RuntimeException("Root message not found"));
                        //extract nested message
                        for (int i = 1; i < paths.length; i++) {
                            final int finalI = i;
                            descriptorBuilder = descriptorBuilder.getNestedTypeBuilderList().stream().filter(
                                    v -> v.getName().equals(finalPaths[finalI])).findFirst()
                                    .orElseThrow(() -> new RuntimeException("Root message not found"));
                        }

                        // find the message's descriptor, convert to builder, and try to add the metadata field.
                        DescriptorProtos.FieldDescriptorProto.Builder metadataField =
                                DescriptorProtos.FieldDescriptorProto.newBuilder();

                        // find the number position of the metadata field.
                        int maxNumber = descriptorBuilder.getFieldList().stream()
                                .map(v -> v.getNumber()).max(Integer::compareTo)
                                .orElse(descriptorBuilder.getFieldCount());
                        metadataField.setName(MESSAGE_METADATA_KEY)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setNumber(maxNumber + 1)
                                .setTypeName(metadataDescriptor.getName());
                        descriptorBuilder.addField(metadataField);

                        rootFileDescriptorProto = rootFileDescriptorProtoBuilder.build();
                        //recursively build FileDescriptor
                        deserializeFileDescriptor(rootFileDescriptorProto,
                                fileDescriptorCache, fileDescriptorProtoCache);
                        //extract root fileDescriptor
                        Descriptors.FileDescriptor fileDescriptor =
                                fileDescriptorCache.get(schemaData.getRootFileDescriptorName());
                        //trim package
                        paths = StringUtils.removeFirst(schemaData.getRootMessageTypeName(),
                                        fileDescriptor.getPackage())
                                .replaceFirst("\\.", "").split("\\.");
                        //extract root message
                        descriptor = fileDescriptor.findMessageTypeByName(paths[0]);
                        //extract nested message
                        for (int i = 1; i < paths.length; i++) {
                            descriptor = descriptor.findNestedTypeByName(paths[i]);
                        }
                    } catch (IOException e) {
                        throw new UnsupportedOperationException("Cannot extract schema from record", e);
                    }
                } else {
                    descriptor = (Descriptors.Descriptor) internalSchema.getNativeSchema().orElse(null);
                }
                log.info("Using protobuf descriptor: {}", descriptor.toProto().getFieldList());
            } else {
                rootAvroSchema = AvroRecordUtil.convertToAvroSchema(schema);
                if (useMetadata) {
                    rootAvroSchema = MetadataUtil.setMetadataSchema(rootAvroSchema, useHumanReadableMessageId,
                            useHumanReadableSchemaVersion, includeTopicToMetadata, includePublishTimeToMetadata,
                            includeMessageKeyToMetadata);
                }
                log.info("Using avro schema: {}", rootAvroSchema);
            }
            if (descriptor == null && rootAvroSchema == null) {
                throw new UnsupportedOperationException("Cannot extract schema from record");
            }
        }
    }

    @Override
    public boolean doSupportPulsarSchemaType(SchemaType schemaType) {
        switch (schemaType) {
            case AVRO:
            case JSON:
            case PROTOBUF:
            case PROTOBUF_NATIVE:
            case KEY_VALUE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public ByteBuffer recordWriterBuf(Iterator<Record<GenericRecord>> records) throws Exception {
        int pageSize = 64 * 1024;
        ParquetWriter<Object> parquetWriter = null;
        S3ParquetOutputFile file = new S3ParquetOutputFile();

        try {
            if (descriptor != null) {
                parquetWriter = ProtobufParquetWriter.builder(file)
                        .withPageSize(pageSize)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withCompressionCodec(compressionCodecName)
                        .withDescriptor(descriptor)
                        .build();
            } else if (rootAvroSchema != null) {
                parquetWriter = AvroParquetWriter
                        .builder(file)
                        .withPageSize(pageSize)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withCompressionCodec(compressionCodecName)
                        .withSchema(rootAvroSchema).build();
            } else {
                throw new UnsupportedOperationException("Cannot init parquet writer");
            }
            while (records.hasNext()) {
                final Record<GenericRecord> next = records.next();
                GenericRecord genericRecord = next.getValue();
                if (genericRecord.getSchemaType() == SchemaType.BYTES
                        && internalSchema.getSchemaInfo().getType() == SchemaType.PROTOBUF_NATIVE) {
                    genericRecord = internalSchema.decode((byte[]) next.getValue().getNativeObject());
                }

                if (genericRecord.getNativeObject() instanceof DynamicMessage) {
                    DynamicMessage protoRecord = (DynamicMessage) genericRecord.getNativeObject();
                    if (useMetadata) {
                        // Add metadata to the record
                        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);
                        Metadata.PulsarIOCSCProtobufMessageMetadata metadata = getMetadataFromMessage(next,
                                useHumanReadableMessageId, useHumanReadableSchemaVersion,
                                includeTopicToMetadata, includePublishTimeToMetadata, includeMessageKeyToMetadata);
                        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
                            if (field.getName().equals(MESSAGE_METADATA_KEY)) {
                                messageBuilder.setField(field, metadata);
                            } else {
                                messageBuilder.setField(field, protoRecord.getField(
                                        protoRecord.getDescriptorForType().findFieldByName(field.getName())));
                            }
                        }

                        protoRecord = messageBuilder.build();
                    }
                    if (parquetWriter != null) {
                        parquetWriter.write(protoRecord);
                    }
                } else {
                    org.apache.avro.generic.GenericRecord writeRecord = AvroRecordUtil
                            .convertGenericRecord(genericRecord, rootAvroSchema);
                    if (useMetadata) {
                        org.apache.avro.generic.GenericRecord metadataRecord =
                                MetadataUtil.extractedMetadataRecord(next,
                                        useHumanReadableMessageId,
                                        useHumanReadableSchemaVersion,
                                        includeTopicToMetadata,
                                        includePublishTimeToMetadata,
                                        includeMessageKeyToMetadata);
                        writeRecord.put(MESSAGE_METADATA_KEY, metadataRecord);
                    }
                    if (parquetWriter != null) {
                        parquetWriter.write(writeRecord);
                    }
                }
            }
        } finally {
            IOUtils.closeQuietly(parquetWriter);
        }
        return ByteBuffer.wrap(file.toByteArray());
    }

    private static void deserializeFileDescriptor(DescriptorProtos.FileDescriptorProto fileDescriptorProto,
                                                  Map<String, Descriptors.FileDescriptor> fileDescriptorCache,
                                                  Map<String, DescriptorProtos.FileDescriptorProto>
                                                          fileDescriptorProtoCache) {
        fileDescriptorProto.getDependencyList().forEach(dependencyFileDescriptorName -> {
            log.info("Deserializing dependency file descriptor: {}", dependencyFileDescriptorName);
            if (!fileDescriptorCache.containsKey(dependencyFileDescriptorName)) {
                DescriptorProtos.FileDescriptorProto dependencyFileDescriptor =
                        fileDescriptorProtoCache.get(dependencyFileDescriptorName);
                deserializeFileDescriptor(dependencyFileDescriptor, fileDescriptorCache, fileDescriptorProtoCache);
            }
        });

        Descriptors.FileDescriptor[] dependencyFileDescriptors =
                fileDescriptorProto.getDependencyList().stream().map(dependency -> {
            if (fileDescriptorCache.containsKey(dependency)) {
                return fileDescriptorCache.get(dependency);
            } else {
                throw new SchemaSerializationException("'" + fileDescriptorProto.getName()
                        + "' can't resolve  dependency '" + dependency + "'.");
            }
        }).toArray(Descriptors.FileDescriptor[]::new);

        try {
            Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto,
                    dependencyFileDescriptors);
            fileDescriptorCache.put(fileDescriptor.getFullName(), fileDescriptor);
        } catch (Descriptors.DescriptorValidationException e) {
            e.printStackTrace();
            throw new SchemaSerializationException(e);
        }
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
