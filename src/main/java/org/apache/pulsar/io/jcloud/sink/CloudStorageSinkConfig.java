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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
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

    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The Azure Blob Storage account SAS token.")
    private String azureStorageAccountSASToken;

    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The Azure Blob Storage account name.")
    private String azureStorageAccountName;

    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The Azure Blob Storage account key.")
    private String azureStorageAccountKey;

    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The Azure Blob Storage account connection string.")
    private String azureStorageAccountConnectionString;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The aws role to use. Implies to use an assume role.")
    private String role;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The aws role session name to use. Implies to use an assume role.")
    private String roleSessionName;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The sts endpoint to use, default to default sts endpoint.")
    private String stsEndpoint;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The sts region to use, defaults to the 'region' config or env region.")
    private String stsRegion;

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
        if (getProvider().equalsIgnoreCase(PROVIDER_AZURE) && isEmpty(azureStorageAccountConnectionString)) {
            checkArgument(isNotBlank(getEndpoint()),
                    "endpoint property must be set.");
        }
    }
}
