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

import static org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig.PROVIDER_GCS;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;
import org.jclouds.domain.Credentials;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * google gcs credential for jclouds.
 */
public class GcsCredential implements JcloudsCredential {

    private static final Logger LOGGER = LoggerFactory.getLogger(GcsCredential.class);

    @Override
    public String provider() {
        return PROVIDER_GCS;
    }

    @Override
    public Supplier<Credentials> getCredentials(CloudStorageSinkConfig sinkConfig) {
        //   for gcs, use downloaded file 'google_creds.json', which contains service account key by
        //     following instructions in page https://support.google.com/googleapi/answer/6158849
        String gcsKeyPath = sinkConfig.getGcsServiceAccountKeyFilePath();
        String gcsKeyContent = sinkConfig.getGcsServiceAccountKeyFileContent();
        if (!StringUtils.isEmpty(gcsKeyPath)) {
            try {
                String loadedContent = Files.toString(
                        new File(gcsKeyPath), Charset.defaultCharset());
                return () -> new GoogleCredentialsFromJson(loadedContent).get();
            } catch (IOException ioe) {
                LOGGER.error("Cannot read GCS service account credentials file: {}", gcsKeyPath);
                throw new RuntimeException(ioe);
            }
        } else if (!StringUtils.isEmpty(gcsKeyContent)) {
            return () -> new GoogleCredentialsFromJson(gcsKeyContent).get();
        } else {
            throw new RuntimeException(
                    "The service account key path and key content is empty for GCS driver");
        }
    }
}
