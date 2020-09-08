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
        config.put("provider", "aws-s3");
        config.put("accessKeyId", "aws-s3");
        config.put("secretAccessKey", "aws-s3");
        config.put("bucket", "testbucket");
        config.put("region", "localhost");
        config.put("formatType", "avro");
        config.put("partitionerType", "default");
        config.put("partitionerParam", "");
        config.put("batchSize", 10);
        S3SinkConfig s3SinkConfig = S3SinkConfig.load(config);
        s3SinkConfig.validate();


        Assert.assertEquals(config.get("provider"), s3SinkConfig.getProvider());
        Assert.assertEquals(config.get("accessKeyId"), s3SinkConfig.getAccessKeyId());
        Assert.assertEquals(config.get("secretAccessKey"), s3SinkConfig.getSecretAccessKey());
        Assert.assertEquals(config.get("bucket"), s3SinkConfig.getBucket());
        Assert.assertEquals(config.get("region"), s3SinkConfig.getRegion());
        Assert.assertEquals(config.get("formatType"), s3SinkConfig.getFormatType());
        Assert.assertEquals(config.get("partitionerType"), s3SinkConfig.getPartitionerType());
        Assert.assertEquals(config.get("partitionerParam"), s3SinkConfig.getPartitionerParam());
        Assert.assertEquals(config.get("batchSize"), s3SinkConfig.getBatchSize());
    }
}
