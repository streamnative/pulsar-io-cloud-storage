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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat.Printer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.util.MetadataUtil;

/**
 * json format.
 */
@Slf4j
public class JsonFormat implements Format<GenericRecord>, InitConfiguration<BlobStoreAbstractConfig> {

    private static final ThreadLocal<ObjectMapper> JSON_MAPPER = ThreadLocal.withInitial(() -> {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    });

    public static final TypeReference<Map<String, Object>> TYPEREF = new TypeReference<Map<String, Object>>() {};

    private boolean useMetadata;
    private boolean useHumanReadableMessageId;
    private boolean useHumanReadableSchemaVersion;

    @Override
    public String getExtension() {
        return ".json";
    }

    @Override
    public void configure(BlobStoreAbstractConfig configuration) {
        this.useMetadata = configuration.isWithMetadata();
        this.useHumanReadableMessageId = configuration.isUseHumanReadableMessageId();
        this.useHumanReadableSchemaVersion = configuration.isUseHumanReadableSchemaVersion();
    }

    @Override
    public void initSchema(Schema<GenericRecord> schema) {
        // noop
    }

    @Override
    public boolean doSupportPulsarSchemaType(SchemaType schemaType) {
        switch (schemaType) {
            case AVRO:
            case JSON:
            case PROTOBUF:
            case PROTOBUF_NATIVE:
            case BYTES:
            case STRING:
                return true;
            default:
                return false;
        }
    }

    @Override
    public ByteBuffer recordWriterBuf(Iterator<Record<GenericRecord>> record) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        while (record.hasNext()) {
            Record<GenericRecord> next = record.next();
            GenericRecord val = next.getValue();
            log.debug("next record {} schema {} val {}", next, next.getSchema(), val);
            Map<String, Object> writeValue = convertRecordToObject(next.getValue());
            if (useMetadata) {
                writeValue.put(MetadataUtil.MESSAGE_METADATA_KEY,
                        MetadataUtil.extractedMetadata(next, useHumanReadableMessageId, useHumanReadableSchemaVersion));
            }
            String recordAsString = JSON_MAPPER.get().writeValueAsString(writeValue);
            stringBuilder.append(recordAsString).append("\n");
        }
        return ByteBuffer.wrap(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
    }

    private Map<String, Object> convertRecordToObject(GenericRecord record) throws IOException {
        if (record.getSchemaType().isStruct()) {
            switch (record.getSchemaType()) {
                case AVRO:
                case JSON:
                case PROTOBUF:
                {
                    List<Field> fields = record.getFields();
                    Map<String, Object> result = new LinkedHashMap<>(fields.size());
                    for (Field field : fields) {
                        String name = field.getName();
                        Object value = record.getField(field);
                        if (value instanceof GenericRecord) {
                            value = convertRecordToObject((GenericRecord) value);
                        }
                        result.put(name, value);
                    }
                    return result;
                }
                case PROTOBUF_NATIVE:
                    if (record.getNativeObject() instanceof DynamicMessage) {
                        Printer printer = com.google.protobuf.util.JsonFormat.printer();
                        String json = printer.print((DynamicMessage) record.getNativeObject());
                        return JSON_MAPPER.get().readValue(json, TYPEREF);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported value schemaType=" + record.getSchemaType());
            }
        } else if (record.getSchemaType() == SchemaType.STRING) {
            return JSON_MAPPER.get().readValue((String) record.getNativeObject(), TYPEREF);
        } else if (record.getSchemaType() == SchemaType.BYTES) {
            return JSON_MAPPER.get().readValue((byte[]) record.getNativeObject(), TYPEREF);
        }
        throw new UnsupportedOperationException("Unsupported value schemaType=" + record.getSchemaType());
    }
}
