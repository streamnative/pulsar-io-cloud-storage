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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/** Consumer-side interpretation only. The original message and wire version are never modified. */
public final class ResolvedSchemaRecord implements Record<GenericRecord> {
    private final Record<GenericRecord> original;
    private final GenericRecord value;
    private final Schema<GenericRecord> schema;
    private final String identity;
    private final org.apache.avro.generic.GenericRecord avro;
    private final AtomicBoolean completed = new AtomicBoolean();

    public ResolvedSchemaRecord(Record<GenericRecord> original) {
        this(original, null, null, null, null);
    }

    ResolvedSchemaRecord(Record<GenericRecord> original, GenericRecord value, Schema<GenericRecord> schema,
                         String identity, org.apache.avro.generic.GenericRecord avro) {
        this.original = original;
        this.value = value;
        this.schema = schema;
        this.identity = identity;
        this.avro = avro;
    }

    public boolean isResolved() {
        return identity != null;
    }

    public String getSchemaIdentity() {
        return identity;
    }

    public org.apache.avro.generic.GenericRecord getAvroRecord() {
        return avro;
    }

    @Override
    public GenericRecord getValue() {
        return isResolved() ? value : original.getValue();
    }

    @Override
    public Schema<GenericRecord> getSchema() {
        return schema != null ? schema : original.getSchema();
    }

    @Override
    public void ack() {
        if (completed.compareAndSet(false, true)) {
            original.ack();
        }
    }

    @Override
    public void fail() {
        if (completed.compareAndSet(false, true)) {
            original.fail();
        }
    }

    /** Shutdown/system failure: release our reference without spending the DLQ retry budget. */
    public void releaseUnacknowledged() {
        if (completed.compareAndSet(false, true)) {
            original.getMessage().ifPresent(Message::release);
        }
    }

    @Override
    public Optional<String> getTopicName() {
        return original.getTopicName();
    }

    @Override
    public Optional<String> getKey() {
        return original.getKey();
    }

    @Override
    public Optional<Long> getEventTime() {
        return original.getEventTime();
    }

    @Override
    public Optional<String> getPartitionId() {
        return original.getPartitionId();
    }

    @Override
    public Optional<Integer> getPartitionIndex() {
        return original.getPartitionIndex();
    }

    @Override
    public Optional<Long> getRecordSequence() {
        return original.getRecordSequence();
    }

    @Override
    public Map<String, String> getProperties() {
        return original.getProperties();
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return original.getDestinationTopic();
    }

    @Override
    public Optional<Message<GenericRecord>> getMessage() {
        return original.getMessage();
    }
}
