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
package org.apache.pulsar.io.jcloud;

import static org.apache.pulsar.io.jcloud.util.MetadataUtil.METADATA_MESSAGE_ID_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
        byte[] messageIdBytes = new byte[]{0x00, 0x01, 0x02, 0x03};
        Record<GenericRecord> mockRecord = mock(Record.class);
        Message<GenericRecord> mockMessage = mock(Message.class);
        MessageId mockMessageId = mock(MessageId.class);
        when(mockRecord.getMessage()).thenReturn(Optional.of(mockMessage));
        when(mockMessage.getMessageId()).thenReturn(mockMessageId);
        when(mockMessage.getProperties()).thenReturn(Collections.emptyMap());
        when(mockMessage.getSchemaVersion()).thenReturn(new byte[]{0x00});
        when(mockMessageId.toString()).thenReturn(messageIdString);
        when(mockMessageId.toByteArray()).thenReturn(messageIdBytes);

        Map<String, Object> metadataWithHumanReadableMessageId =
                MetadataUtil.extractedMetadata(mockRecord, true);
        Assert.assertEquals(metadataWithHumanReadableMessageId.get(METADATA_MESSAGE_ID_KEY),
                messageIdString.getBytes(StandardCharsets.UTF_8));
        Assert.assertNotEquals(metadataWithHumanReadableMessageId.get(METADATA_MESSAGE_ID_KEY),
                ByteBuffer.wrap(messageIdBytes));

        Map<String, Object> metadata = MetadataUtil.extractedMetadata(mockRecord, false);
        Assert.assertEquals(metadata.get(METADATA_MESSAGE_ID_KEY), ByteBuffer.wrap(messageIdBytes));
        Assert.assertNotEquals(metadata.get(METADATA_MESSAGE_ID_KEY),
                messageIdString.getBytes(StandardCharsets.UTF_8));
    }
}
