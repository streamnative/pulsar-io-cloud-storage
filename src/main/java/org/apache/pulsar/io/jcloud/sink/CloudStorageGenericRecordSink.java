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

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.jcloud.credential.JcloudsCredential;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.domain.Credentials;
import org.jclouds.s3.reference.S3Constants;

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

    protected Map<String, JcloudsCredential> jcloudsCredentialMap = loadsCredentials();

    public static Map<String, JcloudsCredential> loadsCredentials(){
        final ServiceLoader<JcloudsCredential> load = ServiceLoader.load(JcloudsCredential.class);
        Map<String, JcloudsCredential> credentialMap = new HashMap<>();
        load.forEach(service -> {
            credentialMap.put(service.provider(), service);
        });
        return credentialMap;
    }

    @Override
    public CloudStorageSinkConfig loadConfig(Map<String, Object> config, SinkContext sinkContext) throws IOException {

        CloudStorageSinkConfig sinkConfig = CloudStorageSinkConfig.load(config);
        checkNotNull(sinkConfig.getAccessKeyId(), "accessKeyId property not set.");
        checkNotNull(sinkConfig.getSecretAccessKey(), "secretAccessKey property not set.");
        return sinkConfig;
    }

    @Override
    protected BlobStoreContext buildBlobStoreContext(CloudStorageSinkConfig sinkConfig) {
        Properties overrides = new Properties();
        overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "25000");
        overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, Integer.toString(100));

        ContextBuilder contextBuilder = ContextBuilder.newBuilder(sinkConfig.getProvider())
                .credentialsSupplier(getCredentialsSupplier(sinkConfig));
        if (!Strings.isNullOrEmpty(sinkConfig.getEndpoint())) {
            contextBuilder.endpoint(sinkConfig.getEndpoint());
            overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
        }
        contextBuilder.overrides(overrides);
        return contextBuilder.buildView(BlobStoreContext.class);
    }

    private Supplier<Credentials> getCredentialsSupplier(CloudStorageSinkConfig sinkConfig) {
        final JcloudsCredential jcloudsCredential =
                jcloudsCredentialMap.getOrDefault(sinkConfig.getProvider(), jcloudsCredentialMap.get("default"));
        return () -> jcloudsCredential.getCredentials(sinkConfig).get();
    }
}
