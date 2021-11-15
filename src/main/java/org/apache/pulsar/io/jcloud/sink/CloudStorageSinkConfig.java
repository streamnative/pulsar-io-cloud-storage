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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;

/**
 * jcloud sink configuration.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
public class CloudStorageSinkConfig extends BlobStoreAbstractConfig {

    private static final long serialVersionUID = 1245636479605735555L;

    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The Cloud Storage access key ID. It requires permission to write objects.")
    private String accessKeyId;

    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The Cloud Storage secret access key.")
    private String secretAccessKey;

    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The GCS service account key file path.")
    private String gcsServiceAccountKeyFilePath;

    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The GCS service account key file content.")
    private String gcsServiceAccountKeyFileContent;

    private String role;

    private String roleSessionName;

    /**
     * If specified, indicates to use the default credentials form the underlying node,
     * rather than using the specified key and secret.
     */
    private boolean useDefaultCredentials = false;

    public static CloudStorageSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), CloudStorageSinkConfig.class);
    }

    public static CloudStorageSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), CloudStorageSinkConfig.class);
    }

    @Override
    public void validate() {
        super.validate();
        if (!useDefaultCredentials && getProvider().equalsIgnoreCase(PROVIDER_AWSS3)) {
            checkNotNull(accessKeyId, "accessKeyId property not set.");
            checkNotNull(secretAccessKey, "secretAccessKey property not set.");
        }
    }
}
