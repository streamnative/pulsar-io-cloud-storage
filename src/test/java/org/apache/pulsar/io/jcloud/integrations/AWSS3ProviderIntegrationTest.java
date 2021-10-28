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
package org.apache.pulsar.io.jcloud.integrations;

import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;
import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.io.jcloud.PulsarTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * AWS S3 integration tests.
 */
@Slf4j
public class AWSS3ProviderIntegrationTest extends PulsarTestBase {

    protected static final String S3MOCK_VERSION = System.getProperty("s3mock.version", "latest");
    protected static final String BUCKET_NAME = "bucket";
    protected static final Collection<String> INITIAL_BUCKET_NAMES = Arrays.asList(BUCKET_NAME);
    protected S3Client s3Client;
    protected PulsarAdmin pulsarAdmin;

    private final S3MockContainer s3Mock =
            new S3MockContainer(S3MOCK_VERSION)
                    .withInitialBuckets(String.join(",", INITIAL_BUCKET_NAMES)).withNetwork(network);

    protected String endpoint;

    @Before
    public void setUp() {
        s3Mock.start();
        // Must create S3Client after S3MockContainer is started, otherwise we can't request the random
        // locally mapped port for the endpoint
        endpoint = s3Mock.getHttpsEndpoint();
        s3Client = createS3ClientV2(endpoint);

        // create connector
        pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getAdminUrl())
                .build();
    }

    @After
    public void tearDown() {
        s3Mock.stop();
    }

    @Test
    public void defaultBucketsGotCreated() {
        final List<Bucket> buckets = s3Client.listBuckets().buckets();
        final Set<String> bucketNames = buckets.stream().map(Bucket::name)
                .filter(INITIAL_BUCKET_NAMES::contains).collect(Collectors.toSet());

        Assert.assertTrue("Not all default Buckets got created", bucketNames.containsAll(INITIAL_BUCKET_NAMES));
    }

    @Test
    public void testSinkConnector() {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setArchive("/pulsar/");
        pulsarAdmin.sinks().createSink();
    }

    protected S3Client createS3ClientV2(String endpoint) {
        return S3Client.builder()
                .region(Region.of("us-east-1"))
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
                .endpointOverride(URI.create(endpoint))
                .httpClient(UrlConnectionHttpClient.builder().buildWithDefaults(
                        AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
                .build();
    }
}
