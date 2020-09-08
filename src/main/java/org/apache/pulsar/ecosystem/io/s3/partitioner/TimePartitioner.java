package org.apache.pulsar.ecosystem.io.s3.partitioner;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.ecosystem.io.s3.BlobStoreAbstractConfig;
import org.apache.pulsar.functions.api.Record;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

public class TimePartitioner<T> implements Partitioner<T> {

    private static final long DEFAULT_PARTITION_DURATION = 24 * 3600 * 1000L;
    private static final String DEFAULT_PARTITION_PATTERN = "yyyy-MM-dd";
    private long partitionDuration;
    private String formatString;
    private DateTimeFormatter dateTimeFormatter;

    @Override
    public void configure(BlobStoreAbstractConfig config) {
        this.formatString = StringUtils.defaultIfBlank(config.getTimePartitionPattern(), DEFAULT_PARTITION_PATTERN);
        this.partitionDuration = parseDurationString(config.getTimePartitionDuration());
        this.dateTimeFormatter = new DateTimeFormatterBuilder()
                .appendPattern(formatString)
                .toFormatter();
    }

    private long parseDurationString(String timePartitionDuration) {
        if (StringUtils.isBlank(timePartitionDuration)){
            return DEFAULT_PARTITION_DURATION;
        }
        String number = timePartitionDuration.substring(0, timePartitionDuration.length() - 1);
        switch (timePartitionDuration.charAt(timePartitionDuration.length() - 1)) {
            case 'd':
                return Integer.parseInt(number) * 24 * 3600 * 1000L;
            case 'h':
                return Integer.parseInt(number) * 3600 * 1000L;
            default:
                throw new RuntimeException("not support timePartitionPattern " + timePartitionDuration);
        }
    }

    @Override
    public String encodePartition(Record<T> sinkRecord) {
        throw new RuntimeException(new IllegalAccessException());
    }

    @Override
    public String encodePartition(Record<T> sinkRecord, long nowInMillis) {
        Message<T> recordMessage = sinkRecord.getMessage().orElseThrow(() -> new RuntimeException("Message not exist"));
        long publishTime = recordMessage.getPublishTime();
        long parsed = (publishTime / partitionDuration) * partitionDuration;
        String timeString = dateTimeFormatter.format(Instant.ofEpochMilli(parsed).atOffset(ZoneOffset.UTC));
        return timeString + PATH_SEPARATOR +
                sinkRecord.getRecordSequence().orElseThrow(() -> new RuntimeException("recordSequence not null"));
    }
}
