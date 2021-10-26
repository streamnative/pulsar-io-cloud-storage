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
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.jcloud.format.AvroFormat;
import org.apache.pulsar.io.jcloud.format.BytesFormat;
import org.apache.pulsar.io.jcloud.format.Format;
import org.apache.pulsar.io.jcloud.format.JsonFormat;
import org.apache.pulsar.io.jcloud.format.ParquetFormat;
import org.apache.pulsar.io.jcloud.partitioner.Partitioner;
import org.apache.pulsar.io.jcloud.partitioner.SimplePartitioner;
import org.apache.pulsar.io.jcloud.partitioner.TimePartitioner;
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
    private static final Map<String, Partitioner<?>> partitionerMap = new ImmutableMap.Builder<String, Partitioner<?>>()
            .put("default", new SimplePartitioner<>())
            .put("time", new TimePartitioner<>())
            .build();

    private String provider;

    private String bucket;

    private String region;

    private String endpoint;

    private String pathPrefix;

    private String formatType;

    private String partitionerType;

    // The AVRO codec.
    // Options: null, deflate, bzip2, xz, zstandard, snappy
    private String avroCodec = "snappy";

    private String timePartitionPattern;

    private String timePartitionDuration;

    private boolean sliceTopicPartitionPath;

    private int batchSize = 10;

    private long batchTimeMs = 1000;

    private boolean withMetadata;
    private boolean withTopicPartitionNumber = true;

    public void validate() {
        checkNotNull(bucket, "bucket property not set.");
        checkNotNull(endpoint, "endpoint property not set.");

        if (!formatMap.containsKey(StringUtils.lowerCase(formatType))) {
            throw new IllegalArgumentException("formatType property not set.");
        }

        if (!partitionerMap.containsKey(StringUtils.lowerCase(partitionerType))) {
            throw new IllegalArgumentException("partitionerType property not set.");
        }
        if ("time".equalsIgnoreCase(partitionerType)) {
            if (StringUtils.isNoneBlank(timePartitionPattern)) {
                LOGGER.info("test timePartitionPattern is ok {} {}",
                        timePartitionPattern,
                        DateTimeFormatter.ofPattern(timePartitionPattern).format(Instant.now().atOffset(ZoneOffset.UTC))
                );
            }
            if (StringUtils.isNoneBlank(timePartitionDuration)) {
                checkArgument(Pattern.matches("^\\d+[dhDH]$", timePartitionDuration), "timePartitionDuration invalid.");
            }
        }
        checkArgument(batchSize > 0, "batchSize property not set.");
        checkArgument(batchTimeMs > 0, "batchTimeMs property not set.");
        if (StringUtils.isNoneBlank(pathPrefix)) {
            checkArgument(!StringUtils.startsWith(pathPrefix, "/"),
                    "pathPrefix cannot start with '/',the style is 'xx/xxx/'.");
            checkArgument(StringUtils.endsWith(pathPrefix, "/"),
                    "pathPrefix must end with '/',the style is 'xx/xxx/'.");
        }
    }

}
