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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.Record;

/**
 * metadata util.
 */
public class MetadataUtil {

    public static final String METADATA_ORIGIN_SCHEMA_KEY = "__originSchema";
    public static final String METADATA_ORIGIN_PROPERTIES_KEY = "__originProperties";
    public static final String METADATA_ORIGIN_SCHEMA_VERSION_KEY = "__originSchemaVersion";
    public static final String METADATA_MESSAGE_ID_KEY = "__messageId";

    public static Map<String, Object> extractedMetadata(Record<GenericRecord> next) {
        Map<String, Object> metadata = new HashMap<>();
        final Message<GenericRecord> message = next.getMessage().get();
        final org.apache.pulsar.client.api.Schema<GenericRecord> recordSchema =
                AvroRecordUtil.extractPulsarSchema(message);
        final SchemaInfo schemaInfo = recordSchema.getSchemaInfo();
        metadata.put(METADATA_ORIGIN_SCHEMA_KEY, schemaInfo.getSchema());
        metadata.put(METADATA_ORIGIN_PROPERTIES_KEY, schemaInfo.getProperties());
        metadata.put(METADATA_ORIGIN_SCHEMA_VERSION_KEY, message.getSchemaVersion());
        metadata.put(METADATA_MESSAGE_ID_KEY, message.getMessageId().toByteArray());
        return metadata;
    }
    public static Schema setMetadataSchema(Schema schema) {

        final List<Schema.Field> fieldWithMetadata = new ArrayList<>();

        schema.getFields().forEach(f -> {
            fieldWithMetadata.add(new Schema.Field(f, f.schema()));
        });

        fieldWithMetadata.add(new Schema.Field(METADATA_ORIGIN_SCHEMA_KEY, Schema.create(Schema.Type.BYTES)));
        fieldWithMetadata.add(new Schema.Field(METADATA_ORIGIN_PROPERTIES_KEY,
                Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createMap(Schema.create(Schema.Type.STRING))
                ))
        );
        fieldWithMetadata.add(new Schema.Field(METADATA_ORIGIN_SCHEMA_VERSION_KEY, Schema.create(Schema.Type.BYTES)));
        fieldWithMetadata.add(new Schema.Field(METADATA_MESSAGE_ID_KEY, Schema.create(Schema.Type.BYTES)));
        return Schema.createRecord(schema.getName(),
                schema.getDoc(),
                schema.getNamespace(),
                schema.isError(),
                fieldWithMetadata
                );
    }
}
