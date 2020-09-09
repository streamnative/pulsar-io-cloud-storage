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
package org.apache.pulsar.ecosystem.io.s3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.ecosystem.io.s3.sink.S3SinkConfig;
import org.junit.Assert;
import org.junit.Test;


/**
 * connector config test.
 */
public class ConnectorConfigTest extends PulsarTestBase {

    @Test
    public void loadBasicConfigTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
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
        S3SinkConfig s3SinkConfig = S3SinkConfig.load(config);
        s3SinkConfig.validate();


        Assert.assertEquals("aws-s3", s3SinkConfig.getProvider());
        Assert.assertEquals(config.get("accessKeyId"), s3SinkConfig.getAccessKeyId());
        Assert.assertEquals(config.get("secretAccessKey"), s3SinkConfig.getSecretAccessKey());
        Assert.assertEquals(config.get("bucket"), s3SinkConfig.getBucket());
        Assert.assertEquals(config.get("region"), s3SinkConfig.getRegion());
        Assert.assertEquals(config.get("formatType"), s3SinkConfig.getFormatType());
        Assert.assertEquals(config.get("partitionerType"), s3SinkConfig.getPartitionerType());
        Assert.assertEquals(config.get("timePartitionPattern"), s3SinkConfig.getTimePartitionPattern());
        Assert.assertEquals(config.get("timePartitionDuration"), s3SinkConfig.getTimePartitionDuration());
        Assert.assertEquals(config.get("batchSize"), s3SinkConfig.getBatchSize());
    }

    @Test
    public void timePartitionDurationTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
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
            S3SinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            config.put("timePartitionDuration", "1000");
            S3SinkConfig.load(config).validate();
            Assert.fail();
        } catch (Exception e) {
        }
        try {
            config.put("timePartitionDuration", "1000h");
            S3SinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            config.put("timePartitionDuration", "1000d");
            S3SinkConfig.load(config).validate();
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            config.put("timePartitionDuration", "1000y");
            S3SinkConfig.load(config).validate();
            Assert.fail();
        } catch (Exception e) {
        }
    }
}
