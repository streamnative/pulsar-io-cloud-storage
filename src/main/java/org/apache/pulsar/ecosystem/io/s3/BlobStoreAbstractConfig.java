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
package org.apache.pulsar.ecosystem.io.s3;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;

import lombok.Data;
import lombok.experimental.Accessors;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.ecosystem.io.s3.format.AvroFormat;
import org.apache.pulsar.ecosystem.io.s3.format.Format;
import org.apache.pulsar.ecosystem.io.s3.format.JsonFormat;
import org.apache.pulsar.ecosystem.io.s3.format.ParquetFormat;
import org.apache.pulsar.ecosystem.io.s3.partitioner.Partitioner;
import org.apache.pulsar.ecosystem.io.s3.partitioner.SimplePartitioner;

/**
 * Configuration object for all Hbase Sink components.
 */
@Data
@Accessors(chain = true)
public class BlobStoreAbstractConfig implements Serializable {

    private static final long serialVersionUID = -8945930873383593712L;

    private static final Map<String, Format<?, ?>> formatMap = new ImmutableMap.Builder<String, Format<?, ?>>()
            .put("avro", new AvroFormat<>())
            .put("json", new JsonFormat<>())
            .put("parquet", new ParquetFormat<>())
            .build();
    private static final Map<String, Partitioner<?>> partitionerMap = new ImmutableMap.Builder<String, Partitioner<?>>()
            .put("default", new SimplePartitioner<>())
            .build();

    private String provider;

    private String bucket;

    private String region;

    private String formatType;

    private String partitionerType;

    private String partitionerParam;

    private int batchSize = 10;

    private long batchTimeMs = 10;


    public void validate() {
        checkNotNull(bucket, "bucket property not set.");
        checkNotNull(region, "region property not set.");

        if (!formatMap.containsKey(StringUtils.lowerCase(formatType))) {
            throw new IllegalArgumentException("formatType property not set.");
        }

        if (!partitionerMap.containsKey(StringUtils.lowerCase(partitionerType))) {
            throw new IllegalArgumentException("partitionerType property not set.");
        }
        checkArgument(batchSize > 0, "batchSize property not set.");
    }

}
