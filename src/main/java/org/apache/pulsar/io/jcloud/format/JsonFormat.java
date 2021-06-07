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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteSource;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.util.MetadataUtil;

/**
 * json format.
 */
public class JsonFormat implements Format<GenericRecord>, InitConfiguration<BlobStoreAbstractConfig> {

    private ObjectMapper objectMapper;

    private boolean useMetadata;

    @Override
    public String getExtension() {
        return ".json";
    }

    @Override
    public void configure(BlobStoreAbstractConfig configuration) {
        this.useMetadata = configuration.isUseMetadata();
    }

    @Override
    public void initSchema(Schema<GenericRecord> schema) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public ByteSource recordWriter(Iterator<Record<GenericRecord>> record) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        if (record.hasNext()) {
            Record<GenericRecord> next = record.next();
            Map<String, Object> writeValue = convertRecordToObject(next.getValue());
            if (useMetadata) {
                writeValue.put(MetadataUtil.MESSAGE_METADATA_KEY, MetadataUtil.extractedMetadata(next));
            }
            String recordAsString = objectMapper.writeValueAsString(writeValue);
            stringBuilder.append(recordAsString).append("\n");
        }
        return ByteSource.wrap(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
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
