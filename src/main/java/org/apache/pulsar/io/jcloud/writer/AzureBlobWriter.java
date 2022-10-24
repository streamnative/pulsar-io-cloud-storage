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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;

/**
 * An implementation of BlobWriter that uses the native Azure-SDK.
 */
@Slf4j
public class AzureBlobWriter implements BlobWriter {

    private final BlobContainerClient containerClient;

    public AzureBlobWriter(CloudStorageSinkConfig sinkConfig) {
        BlobContainerClientBuilder containerClientBuilder = new BlobContainerClientBuilder();
        containerClientBuilder.endpoint(sinkConfig.getEndpoint());
        containerClientBuilder.containerName(sinkConfig.getBucket());

        if (isNotEmpty(sinkConfig.getAzureStorageAccountSASToken())) {
            containerClientBuilder.sasToken(sinkConfig.getAzureStorageAccountSASToken());
        } else if (!isEmptyAccountNameAccountKey(sinkConfig)) {
            containerClientBuilder.credential(new StorageSharedKeyCredential(sinkConfig.getAzureStorageAccountName(),
                    sinkConfig.getAzureStorageAccountKey()));
        } else if (isNotEmpty(sinkConfig.getAzureStorageAccountConnectionString())) {
            containerClientBuilder.credential(StorageSharedKeyCredential.fromConnectionString(
                    sinkConfig.getAzureStorageAccountConnectionString()));
        } else {
            throw new IllegalArgumentException("Either azureStorageAccountSASToken or "
                    + "azureStorageAccountName and azureStorageAccountKey or "
                    + "azureStorageAccountConnectionString must be set.");
        }

        this.containerClient = containerClientBuilder.buildClient();
    }

    @Override
    public void uploadBlob(String key, ByteBuffer payload) throws IOException {
        BlobClient blobClient = this.containerClient.getBlobClient(key);
        blobClient.upload(BinaryData.fromBytes(payload.array()));
    }

    private static boolean isEmptyAccountNameAccountKey(CloudStorageSinkConfig sinkConfig) {
        return (isEmpty(sinkConfig.getAzureStorageAccountName()) || isEmpty(sinkConfig.getAzureStorageAccountKey()));
    }


    @Override
    public void close() throws IOException {

    }
}
