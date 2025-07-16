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

import static com.fasterxml.jackson.core.json.JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
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
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
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

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final TypeReference<Map<String, Object>> TYPEREF = new TypeReference<>() {};
    private static final TypeReference<List<Map<String, Object>>> ARRAY_TYPEREF = new TypeReference<>() {};
    private TypeReference priorityTryTyperef = TYPEREF;

    private boolean useMetadata;
    private boolean useHumanReadableMessageId;
    private boolean useHumanReadableSchemaVersion;
    private boolean includeTopicToMetadata;
    private boolean includePublishTimeToMetadata;
    private boolean includeMessageKeyToMetadata;

    @Override
    public String getExtension() {
        return ".json";
    }

    @Override
    public void configure(BlobStoreAbstractConfig configuration) {
        this.useMetadata = configuration.isWithMetadata();
        this.useHumanReadableMessageId = configuration.isUseHumanReadableMessageId();
        this.useHumanReadableSchemaVersion = configuration.isUseHumanReadableSchemaVersion();
        this.includeTopicToMetadata = configuration.isIncludeTopicToMetadata();
        this.includePublishTimeToMetadata = configuration.isIncludePublishTimeToMetadata();
        this.includeMessageKeyToMetadata = configuration.isIncludeMessageKeyToMetadata();

        if (configuration.isJsonAllowNaN()) {
            JSON_MAPPER.get().enable(ALLOW_NON_NUMERIC_NUMBERS.mappedFeature());
        }
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
            case KEY_VALUE:
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
            final Schema<GenericRecord> schema = next.getSchema();
            log.debug("next record {} schema {} val {}", next, schema, val);
            Map<String, Object> writeValue = convertRecordToObject(val, schema);
            if (useMetadata) {
                writeValue.put(MetadataUtil.MESSAGE_METADATA_KEY,
                        MetadataUtil.extractedMetadata(next, useHumanReadableMessageId, useHumanReadableSchemaVersion,
                                includeTopicToMetadata, includePublishTimeToMetadata, includeMessageKeyToMetadata));
            }
            String recordAsString = JSON_MAPPER.get().writeValueAsString(writeValue);
            stringBuilder.append(recordAsString).append("\n");
        }
        return ByteBuffer.wrap(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
    }

    protected Map<String, Object> convertRecordToObject(GenericRecord record, Schema<?> schema) throws IOException {
        if (record.getSchemaType().isStruct()) {
            switch (record.getSchemaType()) {
                case JSON:
                    JsonNode nativeObject = (JsonNode) record.getNativeObject();
                    return JSON_MAPPER.get().convertValue(nativeObject, TYPEREF);
                case AVRO:
                case PROTOBUF: {
                    List<Field> fields = record.getFields();
                    Map<String, Object> result = new LinkedHashMap<>(fields.size());
                    for (Field field : fields) {
                        String name = field.getName();
                        Object value = record.getField(field);
                        if (value instanceof GenericRecord) {
                            value = convertRecordToObject((GenericRecord) value, schema);
                        }
                        if (value == null) {
                            log.warn("Get filed:{} values is null, maybe data and schema are incompatible", name);
                        }
                        result.put(name, value);
                    }
                    return result;
                }
                case PROTOBUF_NATIVE:
                    if (record.getNativeObject() instanceof DynamicMessage) {
                        Printer printer = com.google.protobuf.util.JsonFormat.printer();
                        String json = printer.print((DynamicMessage) record.getNativeObject());
                        return dynamicReadValue(JSON_FACTORY.createParser(json));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported value schemaType=" + record.getSchemaType());
            }
        } else if (record.getSchemaType() == SchemaType.STRING) {
            return dynamicReadValue(JSON_FACTORY.createParser((String) record.getNativeObject()));
        } else if (record.getSchemaType() == SchemaType.BYTES) {
            return dynamicReadValue(JSON_FACTORY.createParser((byte[]) record.getNativeObject()));
        } else if (record.getSchemaType() == SchemaType.KEY_VALUE) {
            Map<String, Object> jsonKeyValue = new LinkedHashMap<>();
            KeyValueSchema<GenericObject, GenericObject> keyValueSchema = (KeyValueSchema) schema;
            KeyValue<GenericObject, GenericObject> keyValue =
                    (KeyValue<GenericObject, GenericObject>) record.getNativeObject();
            if (keyValue != null) {
                putRecordEntry(jsonKeyValue, "key", keyValue.getKey(), keyValueSchema.getKeySchema());
                putRecordEntry(jsonKeyValue, "value", keyValue.getValue(), keyValueSchema.getValueSchema());
            }
            return jsonKeyValue;
        }
        throw new UnsupportedOperationException("Unsupported value schemaType=" + record.getSchemaType());
    }

    private void putRecordEntry(Map<String, Object> jsonKeyValue, String key,
                                GenericObject record, Schema<?> schema) throws IOException {
        if (record != null) {
            jsonKeyValue.put(key, convertRecordToObject((GenericRecord) record, schema));
        }
    }

    /**
     * This method will try use TYPEREF and ARRAY_TYPEREF to read json value.
     *
     * Once the read is successful, the same type is used for the next read.
     *
     * @param jsonParser
     * @return
     * @throws IOException
     */
    private Map<String, Object> dynamicReadValue(JsonParser jsonParser) throws IOException {
        if (priorityTryTyperef == TYPEREF) {
            try {
                return JSON_MAPPER.get().readValue(jsonParser, TYPEREF);
            } catch (MismatchedInputException e) {
                log.info("Use Map<String, Object> read json failed, try to use List<Map<String, Object>>");
                priorityTryTyperef = ARRAY_TYPEREF;
                return readValueForArrayType(jsonParser);
            }
        } else {
            try {
                return readValueForArrayType(jsonParser);
            } catch (MismatchedInputException e) {
                log.info("Use List<Map<String, Object>> read json failed, try to use Map<String, Object>");
                priorityTryTyperef = TYPEREF;
                return JSON_MAPPER.get().readValue(jsonParser, TYPEREF);
            }
        }
    }

    private Map<String, Object> readValueForArrayType(JsonParser jsonParser) throws IOException {
        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, Object>> valueList =
                JSON_MAPPER.get().readValue(jsonParser, ARRAY_TYPEREF);
        // To maintain compatibility, put the valueList in the map.
        result.put("value", valueList);
        return result;
    }
}
