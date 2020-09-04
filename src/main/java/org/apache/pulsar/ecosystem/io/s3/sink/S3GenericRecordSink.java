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
package org.apache.pulsar.ecosystem.io.s3.sink;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import java.io.IOException;
import java.util.Map;

/**
 * A Simple hbase sink, which interprets input Record in generic record.
 */
@Connector(
    name = "s3",
    type = IOType.SINK,
    help = "The HbaseGenericRecordSink is used for moving messages from Pulsar to Hbase.",
    configClass = S3SinkConfig.class
)
@Slf4j
public class S3GenericRecordSink extends BlobStoreAbstractSink<S3SinkConfig, GenericRecord> {


    @Override
    public S3SinkConfig loadConfig(Map<String, Object> config, SinkContext sinkContext) throws IOException {
        S3SinkConfig sinkConfig = S3SinkConfig.load(config);
        Preconditions.checkNotNull(sinkConfig.getAccessKeyId(), "accessKeyId property not set.");
        Preconditions.checkNotNull(sinkConfig.getSecretAccessKey(), "secretAccessKey property not set.");
        return sinkConfig;
    }

    @Override
    protected BlobStoreContext buildBlobStoreContext(S3SinkConfig sinkConfig) {
        return ContextBuilder.newBuilder(sinkConfig.getRegion())
                .credentials(sinkConfig.getAccessKeyId(), sinkConfig.getSecretAccessKey())
                .buildView(BlobStoreContext.class);
    }
}
