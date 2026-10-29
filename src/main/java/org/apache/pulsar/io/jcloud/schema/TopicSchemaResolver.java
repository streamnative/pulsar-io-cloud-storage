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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.functions.FunctionConfig.ProcessingGuarantees;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jcloud.format.Format;

/** Immutable, bounded cache populated before accepting any messages. No lookup occurs in write(). */
public final class TopicSchemaResolver {
    private final Map<String, SchemalessJsonRecordNormalizer> normalizers;

    public TopicSchemaResolver(TopicSchemaFallbackConfig config, SinkContext context) throws Exception {
        SinkConfig runtime = context.getSinkConfig();
        if (runtime == null || runtime.getProcessingGuarantees() != ProcessingGuarantees.ATLEAST_ONCE
                || !Boolean.FALSE.equals(runtime.getAutoAck()) || runtime.getTransformFunction() != null
                || (runtime.getTimeoutMs() != null && runtime.getTimeoutMs() != 0)) {
            throw new IllegalArgumentException("topicSchemaFallback requires explicit ATLEAST_ONCE, autoAck=false, "
                    + "no ACK timeout, and no transformFunction");
        }
        Map<String, SchemalessJsonRecordNormalizer> loaded = new HashMap<>();
        for (Map.Entry<String, Long> entry : config.getTopicVersions().entrySet()) {
            SchemaInfo info;
            try {
                info = PinnedSchemaLookup.lookup(context.getPulsarClient(), entry.getKey(), entry.getValue(),
                        config.getLookupTimeoutMs());
            } catch (Exception e) {
                try {
                    context.recordMetric("_cloud_storage_schema_lookup_failure_", 1);
                } catch (RuntimeException ignored) {
                    // Observability cannot change delivery semantics.
                }
                throw new IllegalArgumentException("Unable to load pinned topic schema: " + entry.getKey()
                        + " version " + entry.getValue(), e);
            }
            if (info.getType() != SchemaType.JSON || info.getSchema().length > 1024 * 1024) {
                throw new IllegalArgumentException("Fallback requires JSON registry schemas no larger than 1 MiB");
            }
            String digest = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256")
                    .digest(info.getSchemaDefinition().getBytes(StandardCharsets.UTF_8)));
            String identity = "fallback:" + entry.getKey() + ":" + entry.getValue() + ":" + digest;
            loaded.put(entry.getKey(), new SchemalessJsonRecordNormalizer(info, identity, config.getMaxPayloadBytes()));
        }
        normalizers = Collections.unmodifiableMap(loaded);
    }

    public void validateFormat(Format<GenericRecord> format) {
        normalizers.values().forEach(normalizer -> format.initSchema(normalizer.getSchema()));
    }

    public SchemalessJsonRecordNormalizer forRecord(Record<GenericRecord> record) {
        if (record.getValue() == null || record.getValue().getSchemaType() != SchemaType.BYTES
                || record.getMessage().orElseThrow(IllegalArgumentException::new).getSchemaVersion() != null
                || record.getValue().getSchemaVersion() != null || !record.getTopicName().isPresent()) {
            return null;
        }
        String logicalTopic = TopicName.get(record.getTopicName().get()).getPartitionedTopicName();
        return normalizers.get(logicalTopic);
    }
}
