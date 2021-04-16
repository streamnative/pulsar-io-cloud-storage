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
package org.apache.pulsar.io.jcloud.credential;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import java.util.function.Supplier;
import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;
import org.apache.pulsar.io.jcloud.util.CredentialsUtil;
import org.jclouds.aws.domain.SessionCredentials;
import org.jclouds.domain.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * aws s3 credential for jclouds.
 */
public class AwsCredential implements JcloudsCredential {

    private static final Logger LOGGER = LoggerFactory.getLogger(AwsCredential.class);

    @Override
    public String provider() {
        return "aws-s3";
    }

    @Override
    public Supplier<Credentials> getCredentials(CloudStorageSinkConfig sinkConfig) {
        AWSCredentialsProvider credsChain = CredentialsUtil.getAWSCredentialProvider(sinkConfig);
        // try and get creds before starting... if we can't fetch
        // creds on boot, we want to fail
        try {
            credsChain.getCredentials();
        } catch (Exception e) {
            // allowed, some mock jcloud service not need credential
            LOGGER.error("unable to fetch S3 credentials for offloading, failing", e);
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
