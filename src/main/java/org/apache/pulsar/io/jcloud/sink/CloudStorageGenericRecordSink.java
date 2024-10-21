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

import static org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig.PROVIDER_AWSS3V2;
import static org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig.PROVIDER_AZURE;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.jcloud.writer.AzureBlobWriter;
import org.apache.pulsar.io.jcloud.writer.BlobWriter;
import org.apache.pulsar.io.jcloud.writer.JCloudsBlobWriter;
import org.apache.pulsar.io.jcloud.writer.S3BlobWriter;

/**
 * A Simple jcloud sink, which interprets input Record in generic record.
 */
@Connector(
        name = "cloud-storage",
        type = IOType.SINK,
        help = "The CloudStorageGenericRecordSink is used for moving messages from Pulsar to cloud storage.",
        configClass = CloudStorageSinkConfig.class
)
@Slf4j
public class CloudStorageGenericRecordSink extends BlobStoreAbstractSink<CloudStorageSinkConfig> {

    @Override
    public CloudStorageSinkConfig loadConfig(Map<String, Object> config, SinkContext sinkContext) throws IOException {
        CloudStorageSinkConfig sinkConfig =
                IOConfigUtils.loadWithSecrets(config, CloudStorageSinkConfig.class, sinkContext);
        sinkConfig.validate();
        return sinkConfig;
    }

    @Override
    protected BlobWriter initBlobWriter(CloudStorageSinkConfig sinkConfig) {
        //TODO: replace jclouds with native SDK for GCS
        String provider = sinkConfig.getProvider();
        if (provider.equalsIgnoreCase(PROVIDER_AWSS3V2)) {
            return new S3BlobWriter(sinkConfig);
        } else if (provider.equalsIgnoreCase(PROVIDER_AZURE)) {
            return new AzureBlobWriter(sinkConfig);
        } else {
            return new JCloudsBlobWriter(sinkConfig);
        }
    }
}
