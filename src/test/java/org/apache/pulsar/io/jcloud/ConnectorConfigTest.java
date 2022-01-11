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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * connector config test.
 */
public class ConnectorConfigTest {

    @Test
    public void loadBasicConfigTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AWSS3);
        config.put("accessKeyId", "aws-s3");
        config.put("secretAccessKey", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("endpoint", "us-standard");
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
        Assert.assertEquals((int) config.get("batchSize") * 10, cloudStorageSinkConfig.getPendingQueueSize());
    }

    @Test
    public void timePartitionDurationTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_AWSS3);
        config.put("accessKeyId", "aws-s3");
        config.put("secretAccessKey", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("endpoint", "us-standard");
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
            Assert.fail();
        } catch (Exception e) {
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
        config.put("endpoint", "us-standard");
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
    public final void loadFromMapCredentialFromSecretTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("endpoint", "us-standard");
        config.put("formatType", "avro");
        config.put("partitionerType", "default");
        config.put("timePartitionPattern", "yyyy-MM-dd");
        config.put("timePartitionDuration", "2d");
        config.put("batchSize", 10);

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
    }

    @Test
    public final void timePartitionerDurationTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("accessKeyId", "aws-s3");
        config.put("secretAccessKey", "aws-s3");
        config.put("region", "localhost");
        config.put("endpoint", "us-standard");
        config.put("formatType", "avro");
        config.put("partitionerType", "default");
        config.put("timePartitionPattern", "yyyy-MM-dd");
        config.put("batchSize", 10);

        try {
            config.put("timePartitionDuration", "2d");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("timePartitionDuration", "3D");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("timePartitionDuration", "2h");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("timePartitionDuration", "3H");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("timePartitionDuration", "10m");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("timePartitionDuration", "60s");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("timePartitionDuration", "60000");
            CloudStorageSinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
