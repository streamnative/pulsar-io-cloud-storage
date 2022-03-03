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
package org.apache.pulsar.io.jcloud.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * metadata util.
 */
public class MetadataUtil {

    public static final String METADATA_PROPERTIES_KEY = "properties";
    public static final String METADATA_SCHEMA_VERSION_KEY = "schemaVersion";
    public static final String METADATA_MESSAGE_ID_KEY = "messageId";
    public static final String MESSAGE_METADATA_KEY = "__message_metadata__";
    public static final String MESSAGE_METADATA_NAME = "messageMetadata";
    public static final Schema MESSAGE_METADATA = buildMetadataSchema();

    private static ThreadLocal<List<Schema.Field>> schemaFieldThreadLocal = ThreadLocal.withInitial(ArrayList::new);

    public static org.apache.avro.generic.GenericRecord extractedMetadataRecord(Record<GenericRecord> next,
                                                                                boolean useHumanReadableMessageId,
                                                                                boolean useHumanReadableSchemaVersion) {
        final Message<GenericRecord> message = next.getMessage().get();

        GenericData.Record record = new GenericData.Record(buildMetadataSchema(
                useHumanReadableMessageId, useHumanReadableSchemaVersion));
        record.put(METADATA_PROPERTIES_KEY, message.getProperties());
        if (useHumanReadableMessageId) {
            record.put(METADATA_SCHEMA_VERSION_KEY,
                    MetadataUtil.parseSchemaVersionFromBytes(message.getSchemaVersion()));
        } else {
            record.put(METADATA_SCHEMA_VERSION_KEY, ByteBuffer.wrap(message.getSchemaVersion()));
        }
        if (useHumanReadableMessageId) {
            record.put(METADATA_MESSAGE_ID_KEY, message.getMessageId().toString());
        } else {
            record.put(METADATA_MESSAGE_ID_KEY, ByteBuffer.wrap(message.getMessageId().toByteArray()));
        }
        return record;
    }

    public static org.apache.avro.generic.GenericRecord extractedMetadataRecord(Record<GenericRecord> next) {
        return extractedMetadataRecord(next, false, false);
    }

    public static Map<String, Object> extractedMetadata(Record<GenericRecord> next) {
        return extractedMetadata(next, false, false);
    }

    public static Map<String, Object> extractedMetadata(Record<GenericRecord> next,
                                                        boolean useHumanReadableMessageId,
                                                        boolean useHumanReadableSchemaVersion) {
        Map<String, Object> metadata = new HashMap<>();
        final Message<GenericRecord> message = next.getMessage().get();
        metadata.put(METADATA_PROPERTIES_KEY, message.getProperties());
        if (useHumanReadableSchemaVersion) {
            metadata.put(METADATA_SCHEMA_VERSION_KEY,
                    MetadataUtil.parseSchemaVersionFromBytes(message.getSchemaVersion()));
        } else {
            metadata.put(METADATA_SCHEMA_VERSION_KEY, message.getSchemaVersion());
        }
        if (useHumanReadableMessageId) {
            metadata.put(METADATA_MESSAGE_ID_KEY, message.getMessageId().toString());
        } else {
            metadata.put(METADATA_MESSAGE_ID_KEY, ByteBuffer.wrap(message.getMessageId().toByteArray()));
        }
        return metadata;
    }

    public static Schema setMetadataSchema(Schema schema) {
        final List<Schema.Field> fieldWithMetadata = schemaFieldThreadLocal.get();
        fieldWithMetadata.clear();
        schema.getFields().forEach(f -> {
            fieldWithMetadata.add(new Schema.Field(f, f.schema()));
        });
        fieldWithMetadata.add(new Schema.Field(MESSAGE_METADATA_KEY, buildMetadataSchema()));
        return Schema.createRecord(schema.getName(),
                schema.getDoc(),
                schema.getNamespace(),
                schema.isError(),
                fieldWithMetadata
        );
    }

    public static Schema setMetadataSchema(Schema schema,
                                           boolean useHumanReadableMessageId,
                                           boolean useHumanReadableSchemaVersion) {
        final List<Schema.Field> fieldWithMetadata = schemaFieldThreadLocal.get();
        fieldWithMetadata.clear();
        schema.getFields().forEach(f -> {
            fieldWithMetadata.add(new Schema.Field(f, f.schema()));
        });
        fieldWithMetadata.add(new Schema.Field(MESSAGE_METADATA_KEY, buildMetadataSchema(
                useHumanReadableMessageId, useHumanReadableSchemaVersion)));
        return Schema.createRecord(schema.getName(),
                schema.getDoc(),
                schema.getNamespace(),
                schema.isError(),
                fieldWithMetadata
                );
    }

    private static Schema buildMetadataSchema(){
        return buildMetadataSchema(false, false);
    }

    private static Schema buildMetadataSchema(boolean useHumanReadableMessageId,
                                              boolean useHumanReadableSchemaVersion) {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field(METADATA_PROPERTIES_KEY,
                Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createMap(Schema.create(Schema.Type.STRING))
                ))
        );
        if (useHumanReadableSchemaVersion) {
            fields.add(new Schema.Field(METADATA_SCHEMA_VERSION_KEY, Schema.create(Schema.Type.LONG)));
        } else {
            fields.add(new Schema.Field(METADATA_SCHEMA_VERSION_KEY, Schema.create(Schema.Type.BYTES)));
        }
        if (useHumanReadableMessageId) {
            fields.add(new Schema.Field(METADATA_MESSAGE_ID_KEY, Schema.create(Schema.Type.STRING)));
        } else {
            fields.add(new Schema.Field(METADATA_MESSAGE_ID_KEY, Schema.create(Schema.Type.BYTES)));
        }
        return Schema.createRecord(MESSAGE_METADATA_NAME,
                null,
                null,
                false,
                fields
        );
    }

    public static long parseSchemaVersionFromBytes(byte[] schemaVersion) {
        ByteBuffer bb = ByteBuffer.wrap(schemaVersion);
        return bb.getLong();
    }
}
