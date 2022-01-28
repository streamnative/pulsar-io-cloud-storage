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
package org.apache.pulsar.io.jcloud.writer;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;



/**
 * An implementation of BlobWriter that uses the native AWS-SDK, which offers a more direct API
 * and better performance.
 */
public class S3BlobWriter implements BlobWriter {

    private final S3Client s3;
    private final String bucket;
    private ObjectCannedACL acl;

    public S3BlobWriter(CloudStorageSinkConfig sinkConfig) {

        S3ClientBuilder s3Builder = S3Client.builder().credentialsProvider(getCredChain(sinkConfig));
        if (!sinkConfig.getRegion().isEmpty()) {
            s3Builder = s3Builder.region(Region.of(sinkConfig.getRegion()));
        }
        if (!sinkConfig.getEndpoint().isEmpty()) {
            s3Builder = s3Builder.endpointOverride(URI.create(sinkConfig.getEndpoint()));
        }
        s3 = s3Builder.build();

        bucket = sinkConfig.getBucket();
        if (!sinkConfig.getAwsCannedAcl().isEmpty()) {
            acl = ObjectCannedACL.fromValue(sinkConfig.getAwsCannedAcl());
        }
    }

    @Override
    public void uploadBlob(String key, ByteBuffer payload) throws IOException {
        PutObjectRequest.Builder req = PutObjectRequest.builder().bucket(bucket).key(key);
        if (acl != null) {
            req.acl(acl);
        }

        s3.putObject(req.build(), RequestBody.fromByteBuffer(payload));
    }

    @Override
    public void close() throws IOException {
        s3.close();
    }

    private static AwsCredentialsProvider getCredChain(CloudStorageSinkConfig sinkConfig) {
        AwsCredentialsProvider chain = DefaultCredentialsProvider.builder().build();
        if (!sinkConfig.getAccessKeyId().isEmpty() && !sinkConfig.getSecretAccessKey().isEmpty()) {
            chain = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(sinkConfig.getAccessKeyId(), sinkConfig.getSecretAccessKey()));
        }

        if (!sinkConfig.getRole().isEmpty() && !sinkConfig.getRoleSessionName().isEmpty()) {
            AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                    .roleArn(sinkConfig.getRole())
                    .roleSessionName(sinkConfig.getRoleSessionName())
                    .build();
            StsClientBuilder stsb = StsClient.builder().credentialsProvider(chain);
            // try both regions, use the basic region first, then more specific sts region
            if (!sinkConfig.getRegion().isEmpty()) {
                stsb = stsb.region(Region.of(sinkConfig.getRegion()));
            }
            if (!sinkConfig.getStsRegion().isEmpty()) {
                stsb = stsb.region(Region.of(sinkConfig.getStsRegion()));
            }
            if (!sinkConfig.getStsEndpoint().isEmpty()) {
                stsb = stsb.endpointOverride(URI.create(sinkConfig.getStsEndpoint()));
            }

            StsClient stsClient = stsb.build();

            return StsAssumeRoleCredentialsProvider
                    .builder()
                    .stsClient(stsClient).refreshRequest(assumeRoleRequest)
                    .asyncCredentialUpdateEnabled(true)
                    .build();
        }
        return chain;
    }
}
