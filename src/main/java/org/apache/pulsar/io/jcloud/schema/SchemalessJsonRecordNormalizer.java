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
package org.apache.pulsar.io.jcloud.schema;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.util.AvroRecordUtil;

/** Strict JSON-to-Avro conversion before records can enter a shared batch. */
public final class SchemalessJsonRecordNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper(JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(64).build())
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final org.apache.pulsar.client.api.Schema<GenericRecord> schema;
    private final Schema avroSchema;
    private final String identity;
    private final int maxPayloadBytes;

    public SchemalessJsonRecordNormalizer(SchemaInfo info, String identity, int maxPayloadBytes) {
        this.schema = org.apache.pulsar.client.api.Schema.generic(info);
        this.avroSchema = AvroRecordUtil.convertToAvroSchema(schema);
        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Fallback requires a record-shaped JSON schema");
        }
        validateSchema(avroSchema, 0);
        // Validate Parquet's schema conversion at startup, including empty/unsupported record shapes.
        new org.apache.parquet.avro.AvroSchemaConverter().convert(avroSchema);
        this.identity = identity;
        this.maxPayloadBytes = maxPayloadBytes;
    }

    org.apache.pulsar.client.api.Schema<GenericRecord> getSchema() {
        return schema;
    }

    public ResolvedSchemaRecord normalize(Record<GenericRecord> original) throws MalformedRecordException {
        Object nativeValue = original.getValue().getNativeObject();
        if (!(nativeValue instanceof byte[])) {
            throw malformed("non-byte payload");
        }
        byte[] payload = (byte[]) nativeValue;
        if (payload.length == 0 || payload.length > maxPayloadBytes) {
            throw malformed("payload size limit");
        }
        String json;
        try {
            json = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(payload)).toString();
        } catch (CharacterCodingException e) {
            throw malformed("invalid UTF-8");
        }
        JsonNode node;
        try {
            node = MAPPER.readTree(json);
        } catch (IOException e) {
            // Parser exceptions may contain the payload. Never retain them in diagnostics.
            throw malformed("invalid JSON");
        }
        org.apache.avro.generic.GenericRecord avro =
                (org.apache.avro.generic.GenericRecord) convert(node, avroSchema);
        GenericRecord decoded = schema.decode(payload);
        return new ResolvedSchemaRecord(original, decoded, schema, identity, avro);
    }

    private static void validateSchema(Schema schema, int depth) {
        if (depth > 32 || schema.getLogicalType() != null) {
            throw new IllegalArgumentException("Fallback forbids recursive/deep schemas and logical types");
        }
        switch (schema.getType()) {
            case RECORD:
                if (schema.getFields().isEmpty()) {
                    throw new IllegalArgumentException("Fallback forbids empty records");
                }
                schema.getFields().forEach(f -> validateSchema(f.schema(), depth + 1));
                break;
            case ARRAY:
                validateSchema(schema.getElementType(), depth + 1);
                break;
            case MAP:
                validateSchema(schema.getValueType(), depth + 1);
                break;
            case UNION:
                validateSchema(nonNullBranch(schema), depth + 1);
                break;
            case BOOLEAN:
            case STRING:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case ENUM:
                break;
            default:
                throw new IllegalArgumentException("Unsupported fallback schema type: " + schema.getType());
        }
    }

    private static Schema nonNullBranch(Schema schema) {
        if (schema.getTypes().size() != 2
                || schema.getTypes().stream().filter(s -> s.getType() == Schema.Type.NULL).count() != 1) {
            throw new IllegalArgumentException("Fallback only supports nullable single-type unions");
        }
        return schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).findFirst().get();
    }

    private static Object convert(JsonNode node, Schema schema) throws MalformedRecordException {
        if (schema.isUnion()) {
            if (node == null || node.isNull()) {
                return null;
            }
            schema = nonNullBranch(schema);
        }
        if (node == null || node.isNull()) {
            throw malformed("missing or null required value");
        }
        switch (schema.getType()) {
            case RECORD:
                require(node.isObject());
                Iterator<String> names = node.fieldNames();
                while (names.hasNext()) {
                    require(schema.getField(names.next()) != null);
                }
                GenericData.Record record = new GenericData.Record(schema);
                for (Schema.Field field : schema.getFields()) {
                    record.put(field.pos(), convert(node.get(field.name()), field.schema()));
                }
                return record;
            case ARRAY:
                require(node.isArray());
                List<Object> array = new ArrayList<>(node.size());
                for (JsonNode element : node) {
                    array.add(convert(element, schema.getElementType()));
                }
                return array;
            case MAP:
                require(node.isObject());
                Map<String, Object> map = new LinkedHashMap<>();
                Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    map.put(field.getKey(), convert(field.getValue(), schema.getValueType()));
                }
                return map;
            case STRING:
                require(node.isTextual());
                return node.textValue();
            case ENUM:
                require(node.isTextual() && schema.hasEnumSymbol(node.textValue()));
                return new GenericData.EnumSymbol(schema, node.textValue());
            case BOOLEAN:
                require(node.isBoolean());
                return node.booleanValue();
            case INT:
                require(node.isIntegralNumber() && node.canConvertToInt());
                return node.intValue();
            case LONG:
                require(node.isIntegralNumber() && node.canConvertToLong());
                return node.longValue();
            case FLOAT:
                require(node.isNumber() && Float.isFinite(node.floatValue())
                        && (node.floatValue() != 0 || node.decimalValue().signum() == 0));
                return node.floatValue();
            case DOUBLE:
                require(node.isNumber() && Double.isFinite(node.doubleValue())
                        && (node.doubleValue() != 0 || node.decimalValue().signum() == 0));
                return node.doubleValue();
            default:
                throw new IllegalStateException("Unvalidated fallback schema");
        }
    }

    private static void require(boolean condition) throws MalformedRecordException {
        if (!condition) {
            throw malformed("value does not match pinned schema");
        }
    }

    private static MalformedRecordException malformed(String reason) {
        return new MalformedRecordException(reason);
    }

    /** Only this exception denotes a record-level error eligible for explicit drop. */
    public static final class MalformedRecordException extends Exception {
        private static final long serialVersionUID = 1L;

        private MalformedRecordException(String reason) {
            super(reason);
        }
    }
}
