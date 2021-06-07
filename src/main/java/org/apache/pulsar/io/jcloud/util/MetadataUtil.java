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
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.Record;

/**
 * metadata util.
 */
public class MetadataUtil {

    public static final String METADATA_ORIGIN_PROPERTIES_KEY = "__originProperties";
    public static final String METADATA_ORIGIN_SCHEMA_VERSION_KEY = "__originSchemaVersion";
    public static final String METADATA_MESSAGE_ID_KEY = "__messageId";
    public static final String MESSAGE_METADATA_KEY = "__message_metadata__";
    public static final String MESSAGE_METADATA_NAME = "MessageMetadata";
    public static final Schema MESSAGE_METADATA = buildMetadataSchema();

    private static ThreadLocal<List<Schema.Field>> schemaFieldThreadLocal = ThreadLocal.withInitial(ArrayList::new);

    public static org.apache.avro.generic.GenericRecord extractedMetadataRecord(Record<GenericRecord> next) {
        final Message<GenericRecord> message = next.getMessage().get();
        final org.apache.pulsar.client.api.Schema<GenericRecord> recordSchema =
                AvroRecordUtil.extractPulsarSchema(message);
        final SchemaInfo schemaInfo = recordSchema.getSchemaInfo();

        GenericData.Record record = new GenericData.Record(MESSAGE_METADATA);
        record.put(METADATA_ORIGIN_PROPERTIES_KEY, schemaInfo.getProperties());
        record.put(METADATA_ORIGIN_SCHEMA_VERSION_KEY, ByteBuffer.wrap(message.getSchemaVersion()));
        record.put(METADATA_MESSAGE_ID_KEY, ByteBuffer.wrap(message.getMessageId().toByteArray()));
        return record;
    }

    public static Map<String, Object> extractedMetadata(Record<GenericRecord> next) {
        Map<String, Object> metadata = new HashMap<>();
        final Message<GenericRecord> message = next.getMessage().get();
        final org.apache.pulsar.client.api.Schema<GenericRecord> recordSchema =
                AvroRecordUtil.extractPulsarSchema(message);
        final SchemaInfo schemaInfo = recordSchema.getSchemaInfo();
        metadata.put(METADATA_ORIGIN_PROPERTIES_KEY, schemaInfo.getProperties());
        metadata.put(METADATA_ORIGIN_SCHEMA_VERSION_KEY, message.getSchemaVersion());
        metadata.put(METADATA_MESSAGE_ID_KEY, message.getMessageId().toByteArray());


        return metadata;
    }
    public static Schema setMetadataSchema(Schema schema) {
        final List<Schema.Field> fieldWithMetadata = schemaFieldThreadLocal.get();
        fieldWithMetadata.clear();
        schema.getFields().forEach(f -> {
            fieldWithMetadata.add(new Schema.Field(f, f.schema()));
        });
        fieldWithMetadata.add(new Schema.Field(MESSAGE_METADATA_KEY, MESSAGE_METADATA));
        return Schema.createRecord(schema.getName(),
                schema.getDoc(),
                schema.getNamespace(),
                schema.isError(),
                fieldWithMetadata
                );
    }

    private static Schema buildMetadataSchema(){
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field(METADATA_ORIGIN_PROPERTIES_KEY,
                Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createMap(Schema.create(Schema.Type.STRING))
                ))
        );
        fields.add(new Schema.Field(METADATA_ORIGIN_SCHEMA_VERSION_KEY, Schema.create(Schema.Type.BYTES)));
        fields.add(new Schema.Field(METADATA_MESSAGE_ID_KEY, Schema.create(Schema.Type.BYTES)));
        return Schema.createRecord(MESSAGE_METADATA_NAME,
                null,
                null,
                false,
                fields
        );
    }
}
