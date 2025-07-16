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

import static org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig.PROVIDER_AWSS3;
import static org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig.PROVIDER_AZURE;
import static org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig.PROVIDER_GCS;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jcloud.partitioner.PartitionerType;
import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * connector config test.
 */
public class BlobStoreAbstractConfigTest {

    @Test
    public void loadBasicConfigTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AWSS3);
        config.put("accessKeyId", "aws-s3");
        config.put("secretAccessKey", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("endpoint", "https://us-standard");
        config.put("pathPrefix", "pulsar/");
        config.put("formatType", "avro");
        config.put("partitionerType", "default");
        config.put("timePartitionPattern", "yyyy-MM-dd");
        config.put("timePartitionDuration", "2d");
        config.put("batchSize", 10);
        CloudStorageSinkConfig cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        cloudStorageSinkConfig.validate();

        Assert.assertEquals(PROVIDER_AWSS3, cloudStorageSinkConfig.getProvider());
        Assert.assertEquals(config.get("accessKeyId"), cloudStorageSinkConfig.getAccessKeyId());
        Assert.assertEquals(config.get("secretAccessKey"), cloudStorageSinkConfig.getSecretAccessKey());
        Assert.assertEquals(config.get("bucket"), cloudStorageSinkConfig.getBucket());
        Assert.assertEquals(config.get("region"), cloudStorageSinkConfig.getRegion());
        Assert.assertEquals(config.get("formatType"), cloudStorageSinkConfig.getFormatType());
        Assert.assertEquals(config.get("partitionerType"), cloudStorageSinkConfig.getPartitionerType());
        Assert.assertEquals(config.get("timePartitionPattern"), cloudStorageSinkConfig.getTimePartitionPattern());
        Assert.assertEquals(config.get("timePartitionDuration"), cloudStorageSinkConfig.getTimePartitionDuration());
        Assert.assertEquals(config.get("batchSize"), cloudStorageSinkConfig.getBatchSize());
        Assert.assertTrue(cloudStorageSinkConfig.isPartitionerWithTopicName());
        Assert.assertFalse(cloudStorageSinkConfig.isIncludeTopicToMetadata());
        Assert.assertEquals(10000000L, cloudStorageSinkConfig.getMaxBatchBytes());
    }

    @Test
    public void timePartitionDurationTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AWSS3);
        config.put("accessKeyId", "aws-s3");
        config.put("secretAccessKey", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("endpoint", "https://us-standard");
        config.put("formatType", "avro");
        config.put("partitionerType", "time");
        config.put("timePartitionPattern", "yyyy-MM-dd");
        config.put("timePartitionDuration", "2d");
        config.put("batchSize", 10);
        try {
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            config.put("timePartitionDuration", "1000");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            config.put("timePartitionDuration", "1000h");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            config.put("timePartitionDuration", "1000d");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("timePartitionDuration", "1000m");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("timePartitionDuration", "1000s");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("timePartitionDuration", "1000y");
            CloudStorageSinkConfig.load(config).validate();
            Assert.fail();
        } catch (Exception e) {
        }
    }

    @Test
    public void pathPrefixTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AWSS3);
        config.put("accessKeyId", "aws-s3");
        config.put("secretAccessKey", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("endpoint", "https://us-standard");
        config.put("formatType", "avro");
        config.put("partitionerType", "default");
        config.put("timePartitionPattern", "yyyy-MM-dd");
        config.put("timePartitionDuration", "2d");
        config.put("batchSize", 10);

        try {
            config.put("pathPrefix", "pulsar/");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("pathPrefix", "/pulsar/");
            CloudStorageSinkConfig.load(config).validate();
            Assert.fail();
        } catch (Exception e) {
        }

        try {
            config.put("pathPrefix", "/pulsar");
            CloudStorageSinkConfig.load(config).validate();
            Assert.fail();
        } catch (Exception e) {
        }

        try {
            config.put("pathPrefix", "pulsar/test/");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("pathPrefix", "pulsar/test");
            CloudStorageSinkConfig.load(config).validate();
            Assert.fail();
        } catch (Exception e) {
        }
    }

    @Test
    public final void throwOnMalformedEndpointTest() {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AWSS3);
        config.put("accessKeyId", "aws-s3");
        config.put("secretAccessKey", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "us-east-1");
        config.put("endpoint", "s3.us-east-1.amazonaws.com"); // no URI scheme
        config.put("pathPrefix", "pulsar/");
        config.put("formatType", "avro");
        config.put("partitionerType", "default");
        config.put("timePartitionPattern", "yyyy-MM-dd");
        config.put("timePartitionDuration", "2d");
        config.put("batchSize", 10);

        Assert.assertThrows(IllegalArgumentException.class,
                () -> CloudStorageSinkConfig.load(config).validate());
    }

    @Test
    public final void loadFromMapCredentialFromSecretTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("endpoint", "https://us-standard");
        config.put("formatType", "avro");
        config.put("partitionerType", "default");
        config.put("timePartitionPattern", "yyyy-MM-dd");
        config.put("timePartitionDuration", "2d");
        config.put("batchSize", 10);
        config.put("includeMessageKeyToMetadata", true);

        SinkContext sinkContext = Mockito.mock(SinkContext.class);
        Mockito.when(sinkContext.getSecret("accessKeyId"))
                .thenReturn("myKeyId");
        Mockito.when(sinkContext.getSecret("secretAccessKey"))
                .thenReturn("myAccessKey");
        CloudStorageSinkConfig sinkConfig =
                IOConfigUtils.loadWithSecrets(config, CloudStorageSinkConfig.class, sinkContext);

        Assert.assertNotNull(sinkConfig);
        Assert.assertEquals(sinkConfig.getProvider(), "aws-s3");
        Assert.assertEquals(sinkConfig.getBucket(), "testbucket");
        Assert.assertEquals(sinkConfig.getSecretAccessKey(), "myAccessKey");
        Assert.assertEquals(sinkConfig.getAccessKeyId(), "myKeyId");
        Assert.assertTrue(sinkConfig.isIncludeMessageKeyToMetadata());
    }

    @Test
    public void byteConfigTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AWSS3);
        config.put("accessKeyId", "aws-s3");
        config.put("secretAccessKey", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("endpoint", "https://us-standard");
        config.put("pathPrefix", "pulsar/");
        config.put("formatType", "bytes");
        config.put("partitionerType", "default");
        config.put("timePartitionPattern", "yyyy-MM-dd");
        config.put("timePartitionDuration", "2d");
        config.put("batchSize", 10);
        config.put("bytesFormatTypeSeparator", "0x10");
        config.put("partitionerWithTopicName", "false");
        CloudStorageSinkConfig cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        cloudStorageSinkConfig.validate();

        Assert.assertEquals(PROVIDER_AWSS3, cloudStorageSinkConfig.getProvider());
        Assert.assertEquals(config.get("accessKeyId"), cloudStorageSinkConfig.getAccessKeyId());
        Assert.assertEquals(config.get("secretAccessKey"), cloudStorageSinkConfig.getSecretAccessKey());
        Assert.assertEquals(config.get("bucket"), cloudStorageSinkConfig.getBucket());
        Assert.assertEquals(config.get("region"), cloudStorageSinkConfig.getRegion());
        Assert.assertEquals(config.get("formatType"), cloudStorageSinkConfig.getFormatType());
        Assert.assertEquals(config.get("partitionerType"), cloudStorageSinkConfig.getPartitionerType());
        Assert.assertEquals(config.get("timePartitionPattern"), cloudStorageSinkConfig.getTimePartitionPattern());
        Assert.assertEquals(config.get("timePartitionDuration"), cloudStorageSinkConfig.getTimePartitionDuration());
        Assert.assertEquals(config.get("batchSize"), cloudStorageSinkConfig.getBatchSize());
        Assert.assertEquals(config.get("bytesFormatTypeSeparator"),
                cloudStorageSinkConfig.getBytesFormatTypeSeparator());
        Assert.assertFalse(cloudStorageSinkConfig.isPartitionerWithTopicName());
    }

    @Test
    public void testEmptyPartitionerType() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AWSS3);
        config.put("accessKeyId", "aws-s3");
        config.put("secretAccessKey", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("endpoint", "https://localhost");
        config.put("formatType", "bytes");
        CloudStorageSinkConfig cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        try {
            cloudStorageSinkConfig.validate();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("partitionerType property not set properly, available options: partition,time",
                    e.getMessage());
        }
        config.put("partitionerType", "invalid");
        cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        try {
            cloudStorageSinkConfig.validate();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("partitionerType property not set properly, available options: partition,time",
                    e.getMessage());
        }
        config.put("partitionerType", "default");
        cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        cloudStorageSinkConfig.validate();
        for (PartitionerType value : PartitionerType.values()) {
            config.put("partitionerType", value);
            cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
            cloudStorageSinkConfig.validate();
        }
    }

    @Test
    public void testAllowEndpointEmptyWithAzure() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AZURE);
        config.put("azureStorageAccountConnectionString", "test-connection-string");
        config.put("bucket", "test-container-name");
        config.put("formatType", "bytes");
        config.put("partitionerType", "PARTITION");
        CloudStorageSinkConfig cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        try {
            cloudStorageSinkConfig.validate();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNotAllowEndpointEmptyWithAzure() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AZURE);
        config.put("azureStorageAccountSASToken", "test-account-sas-token");
        config.put("bucket", "test-container-name");
        config.put("formatType", "bytes");
        config.put("partitionerType", "PARTITION");
        CloudStorageSinkConfig cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        try {
            cloudStorageSinkConfig.validate();
            Assert.fail("Should be validate failed.");
        } catch (Exception e) {
            Assert.assertEquals("endpoint property must be set.", e.getMessage());
        }
    }

    @Test
    public void testCodec() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AZURE);
        config.put("azureStorageAccountConnectionString", "test-connection-string");
        config.put("bucket", "test-container-name");
        config.put("formatType", "bytes");
        config.put("partitionerType", "PARTITION");
        config.put("avroCodec", "snappy");
        config.put("parquetCodec", "snappy");
        CloudStorageSinkConfig cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        cloudStorageSinkConfig.validate();
        Assert.assertEquals("snappy", cloudStorageSinkConfig.getAvroCodec());
        Assert.assertEquals("snappy", cloudStorageSinkConfig.getParquetCodec());

        config.put("avroCodec", "");
        config.put("parquetCodec", "");
        cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        cloudStorageSinkConfig.validate();
        Assert.assertNull(cloudStorageSinkConfig.getAvroCodec());
        Assert.assertNull(cloudStorageSinkConfig.getParquetCodec());

        config.put("avroCodec", "none");
        config.put("parquetCodec", "none");
        cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        cloudStorageSinkConfig.validate();
        Assert.assertNull(cloudStorageSinkConfig.getAvroCodec());
        Assert.assertNull(cloudStorageSinkConfig.getParquetCodec());
    }

    @Test
    public void loadGoogleCloudStorageProviderTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_GCS);
        config.put("gcsServiceAccountKeyFilePath", "/tmp/gcs.json");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("endpoint", "https://us-standard");
        config.put("pathPrefix", "pulsar/");
        config.put("formatType", "avro");
        config.put("partitionerType", "default");
        config.put("timePartitionPattern", "yyyy-MM-dd");
        config.put("timePartitionDuration", "2d");
        config.put("batchSize", 10);
        config.put("maxBatchBytes", 10000L);
        CloudStorageSinkConfig cloudStorageSinkConfig = CloudStorageSinkConfig.load(config);
        cloudStorageSinkConfig.validate();

        Assert.assertEquals(PROVIDER_GCS, cloudStorageSinkConfig.getProvider());
        Assert.assertEquals(config.get("gcsServiceAccountKeyFilePath"),
                cloudStorageSinkConfig.getGcsServiceAccountKeyFilePath());
        Assert.assertEquals(config.get("bucket"), cloudStorageSinkConfig.getBucket());
        Assert.assertEquals(config.get("region"), cloudStorageSinkConfig.getRegion());
        Assert.assertEquals(config.get("formatType"), cloudStorageSinkConfig.getFormatType());
        Assert.assertEquals(config.get("partitionerType"), cloudStorageSinkConfig.getPartitionerType());
        Assert.assertEquals(config.get("timePartitionPattern"), cloudStorageSinkConfig.getTimePartitionPattern());
        Assert.assertEquals(config.get("timePartitionDuration"), cloudStorageSinkConfig.getTimePartitionDuration());
        Assert.assertEquals(config.get("batchSize"), cloudStorageSinkConfig.getBatchSize());
        Assert.assertEquals(config.get("maxBatchBytes"), cloudStorageSinkConfig.getMaxBatchBytes());
    }

}
