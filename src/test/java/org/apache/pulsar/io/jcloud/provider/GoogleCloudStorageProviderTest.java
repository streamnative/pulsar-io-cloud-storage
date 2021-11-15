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
package org.apache.pulsar.io.jcloud.provider;

import static org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig.PROVIDER_GCS;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * google cloud storage provider test.
 */
public class GoogleCloudStorageProviderTest {
    @Test
    public void loadBasicConfigTest() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("provider", PROVIDER_GCS);
        config.put("gcsServiceAccountKeyFilePath", "/tmp/gcs.json");
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
    }

}
