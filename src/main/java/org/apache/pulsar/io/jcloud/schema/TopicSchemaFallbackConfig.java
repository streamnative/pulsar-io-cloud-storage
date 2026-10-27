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

import static com.google.common.base.Preconditions.checkArgument;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import lombok.Data;
import org.apache.pulsar.common.naming.TopicName;

/** Fixed reader interpretations; never infer a writer version for an unversioned message. */
@Data
public class TopicSchemaFallbackConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private boolean enabled;
    @JsonDeserialize(contentUsing = PinnedVersionDeserializer.class)
    private Map<String, Long> topicVersions = Collections.emptyMap();
    private int maxPayloadBytes = 1024 * 1024;
    private long lookupTimeoutMs = 10000;

    /** Jackson otherwise silently truncates a fractional pin or coerces strings to versions. */
    public static class PinnedVersionDeserializer extends JsonDeserializer<Long> {
        @Override
        public Long deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            if (parser.currentToken() != JsonToken.VALUE_NUMBER_INT) {
                throw JsonMappingException.from(parser, "Pinned registry version must be an integer");
            }
            return parser.getLongValue();
        }
    }

    public void validate() {
        checkArgument(topicVersions != null && !topicVersions.isEmpty() && topicVersions.size() <= 128,
                "topicSchemaFallback.topicVersions must contain 1..128 explicit topics");
        topicVersions.forEach((name, version) -> {
            checkArgument(name != null && name.length() <= 1024, "Invalid fallback topic name");
            TopicName topic = TopicName.get(name);
            checkArgument(name.equals(topic.toString()) && topic.getPartitionIndex() == -1,
                    "Fallback requires full logical topic names, without a partition suffix");
            checkArgument(version != null && version >= 0, "Fallback schema versions must be nonnegative");
        });
        checkArgument(maxPayloadBytes > 0 && maxPayloadBytes <= 16 * 1024 * 1024,
                "topicSchemaFallback.maxPayloadBytes must be 1..16777216");
        checkArgument(lookupTimeoutMs > 0 && lookupTimeoutMs <= 60000,
                "topicSchemaFallback.lookupTimeoutMs must be 1..60000");
    }
}
