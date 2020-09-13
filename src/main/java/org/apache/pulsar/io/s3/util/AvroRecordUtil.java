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
package org.apache.pulsar.io.s3.util;

import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * avro util.
 */
public class AvroRecordUtil {

    public static Schema getAvroSchema(org.apache.pulsar.client.api.Schema<?> pulsarSchema) {
        SchemaInfo schemaInfo = pulsarSchema.getSchemaInfo();
        String rootAvroSchemaString = new String(schemaInfo.getSchema(), StandardCharsets.UTF_8);
        return new Schema.Parser().parse(rootAvroSchemaString);
    }

    public static org.apache.avro.generic.GenericRecord convertGenericRecord(GenericRecord recordValue,
                                                                             Schema rootAvroSchema) {
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
        if (recordValue == null || field == null){
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
