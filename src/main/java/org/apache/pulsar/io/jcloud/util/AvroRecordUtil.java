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

import com.google.protobuf.Descriptors;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * avro util.
 */
public class AvroRecordUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordUtil.class);

    private static final Map<byte[], Schema> SCHEMA_CACHES = new ConcurrentHashMap<>();

    public static Schema getAvroSchema(
            Record<GenericRecord> record) {
        final Message<GenericRecord> message = record.getMessage()
                .orElseThrow(() -> new RuntimeException("Message not exist in record"));

        return SCHEMA_CACHES.computeIfAbsent(message.getSchemaVersion(), (schemaVersion) -> {
            org.apache.pulsar.client.api.Schema<GenericRecord> schema = extractPulsarSchema(message);
            return convertToAvroSchema(schema);
        });
    }

    public static org.apache.pulsar.client.api.Schema<GenericRecord> getPulsarSchema(
            Record<GenericRecord> record) {

        if (record.getSchema() != null) {
            return record.getSchema();
        }
        // Pulsar version < 2.7.0
        final Message<GenericRecord> message = record.getMessage()
                .orElseThrow(() -> new RuntimeException("Message not exist in record, Please check if Source is "
                        + "PulsarSource."));
        return extractPulsarSchema(message);
    }

    public static org.apache.pulsar.client.api.Schema<GenericRecord> extractPulsarSchema(
            Message<GenericRecord> message) {
        try {
            if (message.getReaderSchema().isPresent()) {
                return (org.apache.pulsar.client.api.Schema<GenericRecord>) message.getReaderSchema().get();
            }
            //There is no good way to handle `PulsarRecord#getSchema` in the pulsar function,
            // first read the schema information in the Message through reflection.
            // You can replace this method when the schema is available in the Record.
            final ClassLoader pulsarFunctionClassLoader = message.getClass().getClassLoader();
            Message<GenericRecord> rawMessage = message;
            if (message.getClass().getCanonicalName().endsWith("TopicMessageImpl")){
                final Class<?> classTopicMessageImpl =
                        pulsarFunctionClassLoader.loadClass("org.apache.pulsar.client.impl.TopicMessageImpl");
                final Method getMessage = classTopicMessageImpl.getDeclaredMethod("getMessage");
                @SuppressWarnings("unchecked") final Message<GenericRecord> invoke =
                        (Message<GenericRecord>) getMessage.invoke(message);
                rawMessage = invoke;
            }
            final Class<?> classMessageImpl =
                    pulsarFunctionClassLoader.loadClass("org.apache.pulsar.client.impl.MessageImpl");
            final Method getSchema = classMessageImpl.getDeclaredMethod("getSchema");
            @SuppressWarnings("unchecked")
            org.apache.pulsar.client.api.Schema<GenericRecord> schema =
                    (org.apache.pulsar.client.api.Schema<GenericRecord>) getSchema.invoke(rawMessage);
            return schema;
        } catch (Throwable e) {
            LOGGER.error("getPulsarSchema error", e);
            throw new RuntimeException("getPulsarSchema error", e);
        }
    }

    public static Schema convertToAvroSchema(org.apache.pulsar.client.api.Schema<?> pulsarSchema) {
        SchemaInfo schemaInfo = pulsarSchema.getSchemaInfo();
        if (schemaInfo.getType() == SchemaType.PROTOBUF_NATIVE) {
            Descriptors.Descriptor descriptor = ProtobufNativeSchemaUtils.deserialize(schemaInfo.getSchema());
            ProtobufData model = ProtobufData.get();
            return model.getSchema(descriptor);
        } else {
            String rootAvroSchemaString = schemaInfo.getSchemaDefinition();
            final Schema.Parser parser = new Schema.Parser();
            parser.setValidateDefaults(false);
            return parser.parse(rootAvroSchemaString);
        }
    }

    public static org.apache.avro.generic.GenericRecord convertGenericRecord(GenericRecord recordValue,
                                                                             Schema rootAvroSchema) {
        return convertGenericRecord(recordValue, rootAvroSchema, false);
    }
    public static org.apache.avro.generic.GenericRecord convertGenericRecord(GenericRecord recordValue,
                                                                             Schema rootAvroSchema,
                                                                             boolean useMetadata) {
        org.apache.avro.generic.GenericRecord recordHolder = new GenericData.Record(rootAvroSchema);
        for (org.apache.pulsar.client.api.schema.Field field : recordValue.getFields()) {
            Schema.Field field1 = rootAvroSchema.getField(field.getName());
            Object valueField = readValue(recordValue, field);
            if (valueField instanceof GenericRecord) {

                Schema subSchema = field1.schema();
                if (field1.schema().isUnion()) {
                    subSchema = field1.schema().getTypes().stream()
                            .filter(schema -> schema.getType().equals(Schema.Type.RECORD))
                            .findFirst()
                            .get();
                }
                valueField = convertGenericRecord((GenericRecord) valueField, subSchema);
            }
            recordHolder.put(field.getName(), valueField);
        }
        return recordHolder;
    }

    private static Object readValue(GenericRecord recordValue, org.apache.pulsar.client.api.schema.Field field) {
        if (recordValue == null || field == null) {
            return null;
        }
        //  If the field has no value, NullPointerException will be thrown, for GenericJsonRecord
        try {
            return recordValue.getField(field);
        } catch (NullPointerException ignore) {
            return null;
        }
    }

}
