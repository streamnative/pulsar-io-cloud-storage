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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.junit.Assert;
import org.junit.Test;

public class JsonFormatMethodTest {

    private static final ThreadLocal<ObjectMapper> JSON_MAPPER = ThreadLocal.withInitial(() -> {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    });

    @Test
    public void testJsonIgnoreSchemaRead() throws Exception {

        // 1. Gen GenericRecord with TestSubRecord schema but set to incompatible data.
        Schema<TestRecord.TestSubRecord> subRecordSchema = Schema.JSON(TestRecord.TestSubRecord.class);
        GenericJsonSchema genericRecordSchema = new GenericJsonSchema(subRecordSchema.getSchemaInfo());
        GenericRecordBuilder genericRecordBuilder = genericRecordSchema.newRecordBuilder();
        Map<String, Object> originalData = new HashMap<>();
        originalData.put("incompatibleKey1", "value1");
        originalData.put("incompatibleKey2", 2);
        originalData.forEach(genericRecordBuilder::set);
        GenericRecord genericRecord = genericRecordBuilder.build();

        // 3. Will be converted to the same JSON string as the original data.
        JsonFormat jsonFormat = new JsonFormat();
        BlobStoreAbstractConfig blobStoreAbstractConfig = new BlobStoreAbstractConfig();
        jsonFormat.configure(blobStoreAbstractConfig);
        Map<String, Object> jsonMap = jsonFormat.convertRecordToObject(genericRecord, genericRecordSchema);
        String convertedJsonString = JSON_MAPPER.get().writeValueAsString(jsonMap);
        String originalJsonString = JSON_MAPPER.get().writeValueAsString(originalData);
        Assert.assertEquals(originalJsonString, convertedJsonString);
    }
}
