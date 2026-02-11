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
package org.apache.pulsar.io.jcloud.sink;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;

/**
 * Integration tests for the S3 storage class feature (s3v2 provider) using
 * TestContainers with LocalStack. Requires Docker to be running.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CloudStorageSinkS3IT {

    private static final String TEST_BUCKET = "test-sink-bucket";
    private static final String TEST_TOPIC = "persistent://public/default/test-topic-partition-0";
    private static final DockerImageName LOCALSTACK_IMAGE =
            DockerImageName.parse("localstack/localstack:4.13");

    @Container
    static LocalStackContainer localStack = new LocalStackContainer(LOCALSTACK_IMAGE)
            .withServices(LocalStackContainer.Service.S3);

    private S3Client s3Client;

    @BeforeAll
    void setupS3Client() {
        s3Client = S3Client.builder()
                .endpointOverride(localStack.getEndpointOverride(LocalStackContainer.Service.S3))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                localStack.getAccessKey(),
                                localStack.getSecretKey())))
                .region(Region.of(localStack.getRegion()))
                .serviceConfiguration(
                        software.amazon.awssdk.services.s3.S3Configuration.builder()
                                .pathStyleAccessEnabled(true)
                                .build())
                .build();
    }

    @BeforeEach
    void createBucket() {
        try {
            s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(TEST_BUCKET)
                    .build());
        } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ignored) {
          // no need to recreate existing buckets
        }
    }

    @AfterEach
    void cleanBucket() {
        ListObjectsV2Response listing = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(TEST_BUCKET).build());

        for (S3Object obj : listing.contents()) {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(TEST_BUCKET)
                    .key(obj.key())
                    .build());
        }
    }

    private Map<String, Object> buildSinkConfig(String formatType) {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", "s3v2");
        config.put("accessKeyId", localStack.getAccessKey());
        config.put("secretAccessKey", localStack.getSecretKey());
        config.put("bucket", TEST_BUCKET);
        config.put("region", localStack.getRegion());
        config.put("endpoint",
                localStack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        config.put("formatType", formatType);
        config.put("partitionerType", "partition");
        config.put("batchSize", 2);
        config.put("batchTimeMs", 5000);
        config.put("maxBatchBytes", 10_000_000);
        config.put("pendingQueueSize", 10);
        config.put("withTopicPartitionNumber", true);
        config.put("withMetadata", false);
        return config;
    }

    private Record<GenericRecord> createMockBytesRecord(String payload, long sequenceId) {
        GenericRecord genericRecord = mock(GenericRecord.class);
        when(genericRecord.getSchemaType()).thenReturn(SchemaType.BYTES);
        when(genericRecord.getNativeObject()).thenReturn(payload.getBytes());
        when(genericRecord.getSchemaVersion()).thenReturn(null);
        when(genericRecord.getFields()).thenReturn(java.util.Collections.emptyList());

        Schema<GenericRecord> bytesSchema = mock(Schema.class);
        when(bytesSchema.getSchemaInfo()).thenReturn(
                org.apache.pulsar.common.schema.SchemaInfo.builder()
                        .name("")
                        .type(SchemaType.BYTES)
                        .schema(new byte[0])
                        .build());

        MessageIdAdv messageId = mock(MessageIdAdv.class);
        when(messageId.getLedgerId()).thenReturn(sequenceId);
        when(messageId.getEntryId()).thenReturn(sequenceId);
        when(messageId.getBatchIndex()).thenReturn(0);

        Message<GenericRecord> message = mock(Message.class);
        when(message.getValue()).thenReturn(genericRecord);
        when(message.getData()).thenReturn(payload.getBytes());
        when(message.hasKey()).thenReturn(false);
        when(message.hasIndex()).thenReturn(false);
        when(message.getTopicName()).thenReturn(TEST_TOPIC);
        when(message.getMessageId()).thenReturn(messageId);
        when(message.getProperties()).thenReturn(java.util.Collections.emptyMap());
        when(message.getEventTime()).thenReturn(System.currentTimeMillis());

        Record<GenericRecord> record = mock(Record.class);
        when(record.getValue()).thenReturn(genericRecord);
        when(record.getSchema()).thenReturn(bytesSchema);
        when(record.getMessage()).thenReturn(Optional.of(message));
        when(record.getTopicName()).thenReturn(Optional.of(TEST_TOPIC));
        when(record.getRecordSequence()).thenReturn(Optional.of(sequenceId));
        when(record.getPartitionId()).thenReturn(Optional.of("0"));
        when(record.getPartitionIndex()).thenReturn(Optional.of(0));

        return record;
    }

    private final java.util.concurrent.atomic.AtomicReference<Throwable> fatalError =
            new java.util.concurrent.atomic.AtomicReference<>();

    private SinkContext mockSinkContext() {
        SinkContext ctx = mock(SinkContext.class);
        when(ctx.getSinkName()).thenReturn("cloud-storage-sink-test");
        org.mockito.Mockito.doAnswer(inv -> {
            Throwable t = inv.getArgument(0);
            fatalError.set(t);
            t.printStackTrace();
            return null;
        }).when(ctx).fatal(org.mockito.ArgumentMatchers.any(Throwable.class));
        return ctx;
    }

    private java.util.List<String> listAllObjectKeys() {
        ListObjectsV2Response response = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(TEST_BUCKET).build());
        return response.contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
    }

    private void awaitObjectCount(int expectedMinCount) {
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    java.util.List<String> keys = listAllObjectKeys();
                    assertTrue(keys.size() >= expectedMinCount,
                            "Expected at least " + expectedMinCount + " objects, found: " + keys.size());
                });
    }

    private StorageClass getObjectStorageClass(String key) {
        HeadObjectResponse head = s3Client.headObject(
                HeadObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(key)
                        .build());

        return head.storageClass();
    }

    @Test
    void testDefaultStorageClassIsStandard() throws Exception {
        CloudStorageGenericRecordSink sink = new CloudStorageGenericRecordSink();
        Map<String, Object> config = buildSinkConfig("json");
        SinkContext ctx = mockSinkContext();

        try {
            sink.open(config, ctx);
            sink.write(createMockBytesRecord("{\"key\":\"DefaultSC\"}", 1L));
            sink.write(createMockBytesRecord("{\"key\":\"DefaultSC2\"}", 2L));
            awaitObjectCount(1);
        } finally {
            sink.close();
        }

        java.util.List<String> keys = listAllObjectKeys();
        assertFalse(keys.isEmpty(),
                "Expected objects in S3 with default storage class");

        for (String key : keys) {
            StorageClass sc = getObjectStorageClass(key);
            // S3 returns null storage class for STANDARD objects on HeadObject calls
            assertTrue(sc == null, "Default storage class should be null for key: " + key);
        }
    }

    @Test
    void testExplicitStandardStorageClass() throws Exception {
        CloudStorageGenericRecordSink sink = new CloudStorageGenericRecordSink();
        Map<String, Object> config = buildSinkConfig("json");
        config.put("s3StorageClass", "STANDARD");
        SinkContext ctx = mockSinkContext();

        try {
            sink.open(config, ctx);
            sink.write(createMockBytesRecord("{\"key\":\"Explicit\"}", 1L));
            sink.write(createMockBytesRecord("{\"key\":\"Standard\"}", 2L));
            awaitObjectCount(1);
        } finally {
            sink.close();
        }

        java.util.List<String> keys = listAllObjectKeys();
        assertFalse(keys.isEmpty(),
                "Expected objects in S3 with explicit STANDARD storage class");

        for (String key : keys) {
            StorageClass sc = getObjectStorageClass(key);
            // S3 returns null storage class for STANDARD objects on HeadObject calls
            assertTrue(sc == null, "Storage class should be null for key: " + key);
        }
    }

    @Test
    void testStandardIAStorageClass() throws Exception {
        CloudStorageGenericRecordSink sink = new CloudStorageGenericRecordSink();
        Map<String, Object> config = buildSinkConfig("json");
        config.put("s3StorageClass", "STANDARD_IA");
        SinkContext ctx = mockSinkContext();

        try {
            sink.open(config, ctx);
            sink.write(createMockBytesRecord("{\"key\":\"IA_Record1\"}", 1L));
            sink.write(createMockBytesRecord("{\"key\":\"IA_Record2\"}", 2L));
            awaitObjectCount(1);
        } finally {
            sink.close();
        }

        java.util.List<String> keys = listAllObjectKeys();
        assertFalse(keys.isEmpty(),
                "Expected objects in S3 with STANDARD_IA storage class");

        for (String key : keys) {
            StorageClass sc = getObjectStorageClass(key);
            assertNotNull(sc);
            assertEquals("STANDARD_IA", sc.toString(),
                    "Storage class should be STANDARD_IA for key: " + key);
        }
    }

    @Test
    void testGlacierStorageClass() throws Exception {
        CloudStorageGenericRecordSink sink = new CloudStorageGenericRecordSink();
        Map<String, Object> config = buildSinkConfig("json");
        config.put("s3StorageClass", "GLACIER");
        SinkContext ctx = mockSinkContext();

        try {
            sink.open(config, ctx);
            sink.write(createMockBytesRecord("{\"key\":\"GLACIER_Record1\"}", 1L));
            sink.write(createMockBytesRecord("{\"key\":\"GLACIER_Record2\"}", 2L));
            awaitObjectCount(1);
        } finally {
            sink.close();
        }

        java.util.List<String> keys = listAllObjectKeys();
        assertFalse(keys.isEmpty(),
                "Expected objects in S3 with GLACIER storage class");

        for (String key : keys) {
            StorageClass sc = getObjectStorageClass(key);
            assertNotNull(sc);
            assertEquals("GLACIER", sc.toString(),
                    "Storage class should be GLACIER for key: " + key);
        }
    }

    @Test
    void testIntelligentTieringStorageClass() throws Exception {
        CloudStorageGenericRecordSink sink = new CloudStorageGenericRecordSink();
        Map<String, Object> config = buildSinkConfig("json");
        config.put("s3StorageClass", "INTELLIGENT_TIERING");
        SinkContext ctx = mockSinkContext();

        try {
            sink.open(config, ctx);
            sink.write(createMockBytesRecord("{\"key\":\"IT_Record1\"}", 1L));
            sink.write(createMockBytesRecord("{\"key\":\"IT_Record2\"}", 2L));
            awaitObjectCount(1);
        } finally {
            sink.close();
        }

        java.util.List<String> keys = listAllObjectKeys();
        assertFalse(keys.isEmpty(),
                "Expected objects in S3 with INTELLIGENT_TIERING storage class");

        for (String key : keys) {
            StorageClass sc = getObjectStorageClass(key);
            assertNotNull(sc);
            assertEquals("INTELLIGENT_TIERING", sc.toString(),
                    "Storage class should be INTELLIGENT_TIERING for key: " + key);
        }
    }

    @Test
    void testEmptyStorageClassFailsValidationForS3v2() {
        CloudStorageGenericRecordSink sink = new CloudStorageGenericRecordSink();
        Map<String, Object> config = buildSinkConfig("json");
        config.put("s3StorageClass", "");
        SinkContext ctx = mockSinkContext();

        assertThrows(Exception.class, () -> sink.open(config, ctx),
                "Empty s3StorageClass should fail validation for s3v2 provider");
    }
}
