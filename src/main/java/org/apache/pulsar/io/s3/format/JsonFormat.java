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
package org.apache.pulsar.io.s3.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteSource;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * json format.
 * @param <V> config
 */
public class JsonFormat<V> implements Format<V, Record<GenericRecord>> {

    @Override
    public String getExtension() {
        return ".json";
    }

    @Override
    public ByteSource recordWriter(V config, Record<GenericRecord> record) throws Exception {
        Object writeValue = convertRecordToObject(record.getValue());
        ObjectMapper objectMapper = new ObjectMapper();
        return ByteSource.wrap(objectMapper.writeValueAsString(writeValue).getBytes(StandardCharsets.UTF_8));
    }

    private Map<String, Object> convertRecordToObject(GenericRecord record) {
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
}
