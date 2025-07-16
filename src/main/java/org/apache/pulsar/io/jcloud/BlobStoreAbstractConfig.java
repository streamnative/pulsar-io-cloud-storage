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
package org.apache.pulsar.io.jcloud;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.jcloud.batch.BatchModel;
import org.apache.pulsar.io.jcloud.format.AvroFormat;
import org.apache.pulsar.io.jcloud.format.BytesFormat;
import org.apache.pulsar.io.jcloud.format.Format;
import org.apache.pulsar.io.jcloud.format.JsonFormat;
import org.apache.pulsar.io.jcloud.format.ParquetFormat;
import org.apache.pulsar.io.jcloud.partitioner.PartitionerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Configuration object for all Hbase Sink components.
 */
@Data
@Accessors(chain = true)
public class BlobStoreAbstractConfig implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlobStoreAbstractConfig.class);

    private static final long serialVersionUID = -8945930873383593712L;

    private static final Map<String, Format<?>> formatMap = new ImmutableMap.Builder<String, Format<?>>()
            .put("avro", new AvroFormat())
            .put("json", new JsonFormat())
            .put("parquet", new ParquetFormat())
            .put("bytes", new BytesFormat())
            .build();

    public static final String PROVIDER_AWSS3 = "aws-s3";
    public static final String PROVIDER_AWSS3V2 = "s3v2";
    public static final String PROVIDER_GCS = "google-cloud-storage";
    public static final String PROVIDER_AZURE = "azure-blob-storage";

    // #### bucket configuration ####
    private String provider;
    private String bucket;
    private String region;
    private String endpoint;

    // #### common configuration ####
    private boolean usePathStyleUrl = true;
    private String awsCannedAcl = "";
    private boolean skipFailedMessages = false;

    // #### partitioner configuration ####
    // Options: PARTITION, TIME
    private String partitionerType;
    private String pathPrefix;
    private boolean withTopicPartitionNumber = true;
    private boolean partitionerWithTopicName = true;
    private boolean partitionerUseIndexAsOffset;
    private String timePartitionPattern;
    private String timePartitionDuration;
    private boolean sliceTopicPartitionPath;

    // #### format configuration ####
    private String formatType;
    // The AVRO codec: none, deflate, bzip2, xz, zstandard, snappy
    private String avroCodec = "snappy";
    // The Parquet codec: none, snappy, gzip, lzo, brotli, lz4, zstd
    private String parquetCodec = "gzip";
    private String bytesFormatTypeSeparator = "0x10";
    private boolean jsonAllowNaN = false;

    // #### batch configuration ####
    private long maxBatchBytes = 10_000_000;
    private int batchSize = 10;
    private long batchTimeMs = 1000;
    private BatchModel batchModel = BatchModel.BLEND;
    @Deprecated // never to use
    private int pendingQueueSize = -1;
    @Deprecated // never to use
    private String partitioner;

    // #### metadata configuration ####
    private boolean withMetadata;
    private boolean useHumanReadableMessageId;
    private boolean useHumanReadableSchemaVersion;
    private boolean includeTopicToMetadata;
    private boolean includePublishTimeToMetadata;
    private boolean includeMessageKeyToMetadata;

    public void validate() {
        checkNotNull(provider, "provider not set.");
        checkNotNull(bucket, "bucket property not set.");
        if (provider.equalsIgnoreCase(PROVIDER_AWSS3) || provider.equalsIgnoreCase(PROVIDER_AWSS3V2)) {
            checkArgument(isNotBlank(region) || isNotBlank(endpoint),
                    "Either the aws-end-point or aws-region must be set.");
        }
        if (isNotBlank(endpoint)) {
            checkArgument(hasURIScheme(endpoint), "endpoint property needs to specify URI scheme.");
        }

        if (!formatMap.containsKey(StringUtils.lowerCase(formatType))) {
            throw new IllegalArgumentException("formatType property not set.");
        }

        if (partitionerType == null
                || (EnumUtils.getEnumIgnoreCase(PartitionerType.class, partitionerType) == null
                && !partitionerType.equalsIgnoreCase("default"))) {
            // `default` option is for backward compatibility
            throw new IllegalArgumentException(
                    "partitionerType property not set properly, available options: "
                            + Arrays.stream(PartitionerType.values())
                            .map(Enum::name)
                            .map(String::toLowerCase)
                            .collect(Collectors.joining(","))
            );
        }
        if (PartitionerType.TIME.name().equalsIgnoreCase(partitionerType)) {
            if (StringUtils.isNoneBlank(timePartitionPattern)) {
                LOGGER.info("test timePartitionPattern is ok {} {}",
                        timePartitionPattern,
                        DateTimeFormatter.ofPattern(timePartitionPattern).format(Instant.now().atOffset(ZoneOffset.UTC))
                );
            }
            if (StringUtils.isNoneBlank(timePartitionDuration)) {
                checkArgument(Pattern.matches("^\\d+[dhDHms]?$", timePartitionDuration),
                        "timePartitionDuration invalid.");
            }
        }
        checkArgument(batchSize > 0, "batchSize must be a positive integer.");
        checkArgument(batchTimeMs > 0, "batchTimeMs must be a positive long.");
        checkArgument(maxBatchBytes > 0, "maxBatchBytes must be a positive long.");
        if (StringUtils.isNoneBlank(pathPrefix)) {
            checkArgument(!StringUtils.startsWith(pathPrefix, "/"),
                    "pathPrefix cannot start with '/',the style is 'xx/xxx/'.");
            checkArgument(StringUtils.endsWith(pathPrefix, "/"),
                    "pathPrefix must end with '/',the style is 'xx/xxx/'.");
        }
        pathPrefix = StringUtils.trimToEmpty(pathPrefix);

        if ("bytes".equalsIgnoreCase(formatType)) {
            checkArgument(StringUtils.isNotEmpty(bytesFormatTypeSeparator),
                    "bytesFormatTypeSeparator cannot be empty when formatType is 'bytes'.");
            checkArgument(StringUtils.startsWith(bytesFormatTypeSeparator, "0x"),
                    "bytesFormatTypeSeparator should be a hex encoded string, which starts with '0x'.");
        }

        if (jsonAllowNaN) {
            checkArgument(formatType.equalsIgnoreCase("json"), "jsonAllowNaN can only be true "
                    + "when formatType is 'json'.");
        }

        if (avroCodec != null && (avroCodec.isEmpty() || avroCodec.equals("none"))) {
            avroCodec = null;
        }
        if (parquetCodec != null && (parquetCodec.isEmpty() || parquetCodec.equals("none"))) {
            parquetCodec = null;
        }
    }

    private static boolean hasURIScheme(String endpoint) {
        try {
            URI uri = new URI(endpoint);
            return isNotBlank(uri.getScheme());
        } catch (URISyntaxException e) {
            return false;
        }
    }

}
