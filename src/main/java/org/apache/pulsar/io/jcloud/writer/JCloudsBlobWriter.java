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

import static org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig.PROVIDER_AWSS3;
import static org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig.PROVIDER_GCS;
import static org.jclouds.location.reference.LocationConstants.PROPERTY_REGION;
import static org.jclouds.location.reference.LocationConstants.PROPERTY_REGIONS;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.jcloud.credential.JcloudsCredential;
import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;
import org.apache.pulsar.jcloud.shade.com.google.common.base.Supplier;
import org.apache.pulsar.jcloud.shade.com.google.common.io.ByteSource;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.domain.Credentials;
import org.jclouds.googlecloudstorage.GoogleCloudStorageProviderMetadata;
import org.jclouds.http.HttpResponseException;
import org.jclouds.osgi.ProviderRegistry;
import org.jclouds.s3.reference.S3Constants;


/**
 * An implementation that can write to jclouds for broad support of cloud providers.
 */
@Slf4j
public class JCloudsBlobWriter implements BlobWriter {

    protected BlobStoreContext context;
    protected BlobStore blobStore;
    protected Map<String, JcloudsCredential> jcloudsCredentialMap = loadsCredentials();
    private final String bucketName;

    public JCloudsBlobWriter(CloudStorageSinkConfig sinkConfig) {
        context = this.buildBlobStoreContext(sinkConfig);
        blobStore = context.getBlobStore();

        boolean testCase = "transient".equalsIgnoreCase(sinkConfig.getProvider());
        if (testCase && !blobStore.containerExists(sinkConfig.getBucket())) {
            //test use
            blobStore.createContainerInLocation(null, sinkConfig.getBucket());
        }
        // in the event that we don't have permissions to check for the bucket,
        // don't fail, instead just set an error and continue
        try {
            if (!blobStore.containerExists(sinkConfig.getBucket())) {
                log.error("Bucket {} didn't exist, may be a permissions issue, continuing!", sinkConfig.getBucket());
            } else {
                log.info("Bucket {} exists", sinkConfig.getBucket());
            }
        } catch (HttpResponseException exp) {
            log.error("Bucket {} didn't exist, may be a permissions issue, continuing!", sinkConfig.getBucket());
        }

        bucketName = sinkConfig.getBucket();
    }

    public static Map<String, JcloudsCredential> loadsCredentials() {
        final String path = "META-INF/services/" + JcloudsCredential.class.getName();
        final ClassLoader classLoader = JcloudsCredential.class.getClassLoader();
        InputStream resource = null;
        try {
            resource = classLoader.getResourceAsStream(path);
            final Set<String> classPaths = IOUtils.readLines(resource, Charset.defaultCharset()).stream()
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toSet());
            Map<String, JcloudsCredential> credentialMap = new HashMap<>();
            for (String classPath : classPaths) {
                final JcloudsCredential instance = (JcloudsCredential) classLoader.loadClass(classPath)
                        .getDeclaredConstructor().newInstance();
                credentialMap.put(instance.provider(), instance);
            }
            return credentialMap;
        } catch (Exception e) {
            log.error("load JcloudsCredential implements fail", e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(resource);
        }
    }

    protected BlobStoreContext buildBlobStoreContext(CloudStorageSinkConfig sinkConfig) {
        Properties overrides = new Properties();
        overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "25000");
        overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, Integer.toString(100));

        if (sinkConfig.getProvider().equalsIgnoreCase(PROVIDER_GCS)) {
            ProviderRegistry.registerProvider(new GoogleCloudStorageProviderMetadata());
        }

        ContextBuilder contextBuilder = ContextBuilder.newBuilder(sinkConfig.getProvider())
                .credentialsSupplier(getCredentialsSupplier(sinkConfig));

        if (StringUtils.isNotEmpty(sinkConfig.getEndpoint())) {
            contextBuilder.endpoint(sinkConfig.getEndpoint());
             if (sinkConfig.getProvider().equalsIgnoreCase(PROVIDER_AWSS3)) {
                 if (sinkConfig.isUsePathStyleUrl()) {
                     overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "true");
                 }
             }
        }

        if (StringUtils.isNotEmpty(sinkConfig.getRegion())) {
            overrides.setProperty(PROPERTY_REGION, sinkConfig.getRegion());
             if (sinkConfig.getProvider().equalsIgnoreCase(PROVIDER_AWSS3)) {
                 // For AWS-S3, jclouds will use a full list of AWS regions and run GetBucketLocation query first.
                 // This requires additional permission on GetBucketLocation
                 // If user provided region in config, the connector should not do GetBucketLocation anymore.
                 overrides.setProperty(PROPERTY_REGIONS, sinkConfig.getRegion());
             }
        }

        contextBuilder.overrides(overrides);
        log.info("getOverrides: {}", overrides);
        return contextBuilder.buildView(BlobStoreContext.class);
    }

    private Supplier<Credentials> getCredentialsSupplier(CloudStorageSinkConfig sinkConfig) {
        final JcloudsCredential jcloudsCredential =
                jcloudsCredentialMap.getOrDefault(sinkConfig.getProvider(), jcloudsCredentialMap.get("default"));
        return () -> jcloudsCredential.getCredentials(sinkConfig).get();
    }

    @Override
    public void uploadBlob(String key, ByteBuffer payload) throws IOException {
        log.debug("using raw blob store for uploading");
        Blob blob = blobStore.blobBuilder(key)
                .payload(ByteSource.wrap(payload.array()))
                .contentLength(payload.array().length)
                .build();
        blobStore.putBlob(bucketName, blob);
        blob.getPayload().release();
    }

    @Override
    public void close() throws IOException {
        context.close();
    }
}
