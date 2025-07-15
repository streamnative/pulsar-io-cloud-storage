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
package org.apache.pulsar.io.jcloud.utils;

import static org.apache.pulsar.io.jcloud.util.MetadataUtil.METADATA_MESSAGE_ID_KEY;
import static org.apache.pulsar.io.jcloud.util.MetadataUtil.METADATA_SCHEMA_VERSION_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.util.MetadataUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * metadata utils unit tests.
 */
public class MetadataUtilTest {
    @Test
    public void testExtractedMetadata() throws IOException {
        String messageIdString = "1:2:3:4";
        byte[] schemaVersionBytes = new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A};
        byte[] messageIdBytes = new byte[]{0x00, 0x01, 0x02, 0x03};
        String topicName = "test-topic";
        String messageKey = "test-message-key";
        long publishTime = System.currentTimeMillis();
        Record<GenericRecord> mockRecord = mock(Record.class);
        Message<GenericRecord> mockMessage = mock(Message.class);
        when(mockMessage.getPublishTime()).thenReturn(publishTime);
        MessageId mockMessageId = mock(MessageId.class);
        when(mockRecord.getMessage()).thenReturn(Optional.of(mockMessage));
        when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        when(mockMessage.getProperties()).thenReturn(Collections.emptyMap());
        when(mockMessage.getSchemaVersion()).thenReturn(schemaVersionBytes);
        when(mockMessageId.toString()).thenReturn(messageIdString);
        when(mockMessageId.toByteArray()).thenReturn(messageIdBytes);
        when(mockMessage.getTopicName()).thenReturn(topicName);
        when(mockMessage.getKey()).thenReturn(messageKey);

        Map<String, Object> metadataWithHumanReadableMetadata =
                MetadataUtil.extractedMetadata(mockRecord, true, true, true, true, true);
        Assert.assertEquals(metadataWithHumanReadableMetadata.get(METADATA_MESSAGE_ID_KEY), messageIdString);
        Assert.assertNotEquals(metadataWithHumanReadableMetadata.get(METADATA_MESSAGE_ID_KEY),
                ByteBuffer.wrap(messageIdBytes));
        Assert.assertEquals(metadataWithHumanReadableMetadata.get(METADATA_SCHEMA_VERSION_KEY),
                MetadataUtil.parseSchemaVersionFromBytes(schemaVersionBytes));
        Assert.assertEquals(metadataWithHumanReadableMetadata.get(MetadataUtil.METADATA_TOPIC), topicName);
        Assert.assertEquals(metadataWithHumanReadableMetadata.get(MetadataUtil.METADATA_PUBLISH_TIME), publishTime);
        Assert.assertEquals(metadataWithHumanReadableMetadata.get(MetadataUtil.MESSAGE_METADATA_MESSAGE_KEY),
                messageKey);

        Map<String, Object> metadataWithHumanReadableMessageId =
                MetadataUtil.extractedMetadata(mockRecord, true, false, false, false, false);
        Assert.assertEquals(metadataWithHumanReadableMessageId.get(METADATA_MESSAGE_ID_KEY), messageIdString);
        Assert.assertNotEquals(metadataWithHumanReadableMessageId.get(METADATA_MESSAGE_ID_KEY),
                ByteBuffer.wrap(messageIdBytes));


        Map<String, Object> metadata = MetadataUtil.extractedMetadata(mockRecord, false, false, false, false, false);
        Assert.assertEquals(metadata.get(METADATA_MESSAGE_ID_KEY), ByteBuffer.wrap(messageIdBytes));
        Assert.assertEquals(metadata.get(METADATA_SCHEMA_VERSION_KEY), schemaVersionBytes);
        Assert.assertNotEquals(metadata.get(METADATA_MESSAGE_ID_KEY), messageIdString);
    }
}
