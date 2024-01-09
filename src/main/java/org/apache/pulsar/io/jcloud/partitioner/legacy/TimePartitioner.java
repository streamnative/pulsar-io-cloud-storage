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
package org.apache.pulsar.io.jcloud.partitioner.legacy;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * partition by day, hour.
 *
 * @param <T>
 */
public class TimePartitioner<T> extends AbstractPartitioner<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimePartitioner.class);

    private static final long DEFAULT_PARTITION_DURATION = 24 * 3600 * 1000L;
    private static final String DEFAULT_PARTITION_PATTERN = "yyyy-MM-dd";
    private long partitionDuration;
    private String formatString;
    private DateTimeFormatter dateTimeFormatter;

    @Override
    public void configure(BlobStoreAbstractConfig config) {
        super.configure(config);
        this.formatString = StringUtils.defaultIfBlank(config.getTimePartitionPattern(), DEFAULT_PARTITION_PATTERN);
        this.partitionDuration = parseDurationString(config.getTimePartitionDuration());
        this.dateTimeFormatter = new DateTimeFormatterBuilder()
                .appendPattern(formatString)
                .toFormatter();
    }

    private long parseDurationString(String timePartitionDuration) {
        if (StringUtils.isBlank(timePartitionDuration)) {
            return DEFAULT_PARTITION_DURATION;
        }
        if (Character.isAlphabetic(timePartitionDuration.charAt(timePartitionDuration.length() - 1))) {
            String number = timePartitionDuration.substring(0, timePartitionDuration.length() - 1);
            switch (timePartitionDuration.charAt(timePartitionDuration.length() - 1)) {
                case 'd':
                case 'D':
                    return Long.parseLong(number) * 24L * 3600L * 1000L;
                case 'h':
                case 'H':
                    return Long.parseLong(number) * 3600L * 1000L;
                case 'm':
                    return Long.parseLong(number) * 60L * 1000L;
                case 's':
                    return Long.parseLong(number) * 1000L;
                default:
                    throw new RuntimeException("not supported time duration scale " + timePartitionDuration);
            }
        } else {
            try {
                return Long.parseLong(timePartitionDuration);
            } catch (NumberFormatException ex) {
                throw new RuntimeException("not supported time duration format " + timePartitionDuration, ex);
            }
        }
    }

    @Override
    public String encodePartition(Record<T> sinkRecord) {
        throw new RuntimeException(new IllegalAccessException());
    }

    @Override
    public String encodePartition(Record<T> sinkRecord, long nowInMillis) {
        long publishTime = getPublishTime(sinkRecord, nowInMillis);
        long parsed = (publishTime / partitionDuration) * partitionDuration;
        String timeString = dateTimeFormatter.format(Instant.ofEpochMilli(parsed).atOffset(ZoneOffset.UTC));
        final String result = timeString
                + PATH_SEPARATOR
                + getMessageOffset(sinkRecord);
        return result;
    }

    private long getPublishTime(Record<T> sinkRecord, Long defaultTime) {
        final Supplier<Long> defaultTimeSupplier = () -> {
            LOGGER.warn("record not exist Message {}", sinkRecord.getRecordSequence().get());
            return defaultTime;
        };
        return sinkRecord.getMessage()
                .map(Message::getPublishTime)
                .orElseGet(defaultTimeSupplier);
    }
}
