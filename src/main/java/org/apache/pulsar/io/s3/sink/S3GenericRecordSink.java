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
package org.apache.pulsar.io.s3.sink;

import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.s3.util.CredentialsUtil;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.aws.domain.SessionCredentials;
import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.domain.Credentials;
import org.jclouds.osgi.ApiRegistry;
import org.jclouds.osgi.ProviderRegistry;
import org.jclouds.s3.S3ApiMetadata;
import org.jclouds.s3.reference.S3Constants;

/**
 * A Simple s3 sink, which interprets input Record in generic record.
 */
@Connector(
    name = "s3",
    type = IOType.SINK,
    help = "The S3GenericRecordSink is used for moving messages from Pulsar to S3.",
    configClass = S3SinkConfig.class
)
@Slf4j
public class S3GenericRecordSink extends BlobStoreAbstractSink<S3SinkConfig> {

    @Override
    public S3SinkConfig loadConfig(Map<String, Object> config, SinkContext sinkContext) throws IOException {

        S3SinkConfig sinkConfig = S3SinkConfig.load(config);
        checkNotNull(sinkConfig.getAccessKeyId(), "accessKeyId property not set.");
        checkNotNull(sinkConfig.getSecretAccessKey(), "secretAccessKey property not set.");
        return sinkConfig;
    }

    @Override
    protected BlobStoreContext buildBlobStoreContext(S3SinkConfig sinkConfig) {
        Properties overrides = new Properties();
        overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "25000");
        overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, Integer.toString(100));

        ApiRegistry.registerApi(new S3ApiMetadata());
        ProviderRegistry.registerProvider(new AWSS3ProviderMetadata());

        ContextBuilder contextBuilder = ContextBuilder.newBuilder(sinkConfig.getProvider())
                .credentialsSupplier(getCredentialsSupplier(sinkConfig));
        if (!Strings.isNullOrEmpty(sinkConfig.getEndpoint())) {
            contextBuilder.endpoint(sinkConfig.getEndpoint());
            overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
        }
        contextBuilder.overrides(overrides);
        return contextBuilder.buildView(BlobStoreContext.class);
    }

    private Supplier<Credentials> getCredentialsSupplier(S3SinkConfig sinkConfig) {
        AWSCredentialsProvider credsChain = CredentialsUtil.getAWSCredentialProvider(sinkConfig);
        // try and get creds before starting... if we can't fetch
        // creds on boot, we want to fail
        try {
            credsChain.getCredentials();
        } catch (Exception e) {
            // allowed, some mock s3 service not need credential
            log.error("unable to fetch S3 credentials for offloading, failing", e);
            throw e;
        }

        return () -> {
            AWSCredentials creds = credsChain.getCredentials();
            if (creds == null) {
                // we don't expect this to happen, as we
                // successfully fetched creds on boot
                throw new RuntimeException("Unable to fetch S3 credentials after start, unexpected!");
            }
            // if we have session credentials, we need to send the session token
            // this allows us to support EC2 metadata credentials
            if (creds instanceof AWSSessionCredentials) {
                return SessionCredentials.builder()
                        .accessKeyId(creds.getAWSAccessKeyId())
                        .secretAccessKey(creds.getAWSSecretKey())
                        .sessionToken(((AWSSessionCredentials) creds).getSessionToken())
                        .build();
            } else {
                return new Credentials(creds.getAWSAccessKeyId(), creds.getAWSSecretKey());
            }
        };
    }
}
