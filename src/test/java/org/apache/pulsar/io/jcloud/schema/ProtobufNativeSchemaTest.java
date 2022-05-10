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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.format.ParquetFormat;
import org.apache.pulsar.io.jcloud.schema.proto.Test.TestMessage;
import org.junit.Test;

/**
 * ProtobufNativeSchema tests.
 */
public class ProtobufNativeSchemaTest {
    @Test
    public void parquetTest() {
        Record<GenericRecord> r = mock(Record.class);
        GenericProtobufNativeRecord record = mock(GenericProtobufNativeRecord.class);
        org.apache.pulsar.client.api.Schema<GenericRecord> schema = mock(ProtobufNativeSchema.class);
        SchemaInfo schemaInfo = mock(SchemaInfoImpl.class);
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        TestMessage.Builder msg = TestMessage.newBuilder();
        msg.setStringField("test");
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(msg.build()).build();
        when(record.getNativeObject()).thenReturn(dynamicMessage);
        when(record.getProtobufRecord()).thenReturn(dynamicMessage);
        when(record.getSchemaType()).thenReturn(SchemaType.PROTOBUF_NATIVE);
        when(schema.getSchemaInfo()).thenReturn(schemaInfo);
        when(schema.getNativeSchema()).thenReturn(Optional.of(descriptor));
        when(schemaInfo.getSchema()).thenReturn(ProtobufNativeSchemaUtils.serialize(descriptor));
        when(schemaInfo.getType()).thenReturn(SchemaType.PROTOBUF_NATIVE);
        when(r.getValue()).thenReturn(record);
        try {
            ParquetFormat parquetFormat = new ParquetFormat();
            parquetFormat.initSchema(schema);
            ByteBuffer bytes = parquetFormat.recordWriterBuf(Lists.newArrayList(r).iterator());
            assertNotEquals(0, bytes.array().length);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void parquetWithMetadataTest() {
        Record<GenericRecord> r = mock(Record.class);
        GenericProtobufNativeRecord record = mock(GenericProtobufNativeRecord.class);
        org.apache.pulsar.client.api.Schema<GenericRecord> schema = mock(ProtobufNativeSchema.class);
        SchemaInfo schemaInfo = mock(SchemaInfoImpl.class);
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        TestMessage.Builder msg = TestMessage.newBuilder();
        msg.setStringField("test");
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(msg.build()).build();
        when(record.getNativeObject()).thenReturn(dynamicMessage);
        when(record.getProtobufRecord()).thenReturn(dynamicMessage);
        when(record.getSchemaType()).thenReturn(SchemaType.PROTOBUF_NATIVE);
        when(schema.getSchemaInfo()).thenReturn(schemaInfo);
        when(schema.getNativeSchema()).thenReturn(Optional.of(descriptor));
        when(schemaInfo.getSchema()).thenReturn(ProtobufNativeSchemaUtils.serialize(descriptor));
        when(schemaInfo.getType()).thenReturn(SchemaType.PROTOBUF_NATIVE);
        when(r.getValue()).thenReturn(record);

        Message m = mock(Message.class);
        when(r.getMessage()).thenReturn(Optional.of(m));
        when(m.getMessageId()).thenReturn(new MessageIdImpl(12, 34, 1));
        when(m.getSchemaVersion()).thenReturn(new byte[] { 1 });
        when(m.getPublishTime()).thenReturn(System.currentTimeMillis());
        when(m.getEventTime()).thenReturn(System.currentTimeMillis());
        when(m.getSequenceId()).thenReturn(1L);

        BlobStoreAbstractConfig config = mock(BlobStoreAbstractConfig.class);
        when(config.isWithMetadata()).thenReturn(true);
        try {
            ParquetFormat parquetFormat = new ParquetFormat();
            parquetFormat.configure(config);
            parquetFormat.initSchema(schema);
            ByteBuffer bytes = parquetFormat.recordWriterBuf(Lists.newArrayList(r).iterator());
            assertNotEquals(0, bytes.array().length);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
