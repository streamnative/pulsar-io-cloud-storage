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
import static org.apache.pulsar.io.jcloud.util.AvroRecordUtil.getPulsarSchema;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.format.AvroFormat;
import org.apache.pulsar.io.jcloud.format.BytesFormat;
import org.apache.pulsar.io.jcloud.format.Format;
import org.apache.pulsar.io.jcloud.format.InitConfiguration;
import org.apache.pulsar.io.jcloud.format.JsonFormat;
import org.apache.pulsar.io.jcloud.format.ParquetFormat;
import org.apache.pulsar.io.jcloud.partitioner.Partitioner;
import org.apache.pulsar.io.jcloud.partitioner.PartitionerType;
import org.apache.pulsar.io.jcloud.partitioner.SimplePartitioner;
import org.apache.pulsar.io.jcloud.partitioner.TimePartitioner;
import org.apache.pulsar.io.jcloud.writer.BlobWriter;
import org.jclouds.blobstore.ContainerNotFoundException;

/**
 * A Simple abstract class for BlobStore sink.
 * Users need to implement extractKeyValue function to use this sink
 */
@Slf4j
public abstract class BlobStoreAbstractSink<V extends BlobStoreAbstractConfig> implements Sink<GenericRecord> {

    private V sinkConfig;

    protected Partitioner<GenericRecord> partitioner;

    protected Format<GenericRecord> format;

    private final ScheduledExecutorService flushExecutor =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setNameFormat("pulsar-io-cloud-storage-sink-flush-%d")
                .build());;

    private String pathPrefix;

    private long maxBatchSize;
    private final AtomicLong currentBatchSize = new AtomicLong(0L);
    private ArrayBlockingQueue<Record<GenericRecord>> pendingFlushQueue;
    private final AtomicBoolean isFlushRunning = new AtomicBoolean(false);
    private SinkContext sinkContext;
    private volatile boolean isRunning = false;

    private BlobWriter blobWriter;

    private static final String METRICS_TOTAL_SUCCESS = "_cloud_storage_sink_total_success_";
    private static final String METRICS_TOTAL_FAILURE = "_cloud_storage_sink_total_failure_";
    private static final String METRICS_LATEST_UPLOAD_ELAPSED_TIME = "_cloud_storage_latest_upload_elapsed_time_";

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        sinkConfig = loadConfig(config, sinkContext);
        sinkConfig.validate();
        pendingFlushQueue = new ArrayBlockingQueue<>(sinkConfig.getPendingQueueSize());
        format = buildFormat(sinkConfig);
        if (format instanceof InitConfiguration) {
            InitConfiguration<BlobStoreAbstractConfig> formatConfigInitializer =
                (InitConfiguration<BlobStoreAbstractConfig>) format;
            formatConfigInitializer.configure(sinkConfig);
        }
        partitioner = buildPartitioner(sinkConfig);
        pathPrefix = StringUtils.trimToEmpty(sinkConfig.getPathPrefix());
        long batchTimeMs = sinkConfig.getBatchTimeMs();
        maxBatchSize = sinkConfig.getBatchSize();
        flushExecutor.scheduleWithFixedDelay(this::flush, batchTimeMs, batchTimeMs, TimeUnit.MILLISECONDS);
        isRunning = true;
        this.sinkContext = sinkContext;
        this.blobWriter = initBlobWriter(sinkConfig);
    }

    private void flushIfNeeded(boolean force) {
        if (isFlushRunning.get()) {
            return;
        }
        if (force || currentBatchSize.get() >= maxBatchSize) {
            flushExecutor.submit(this::flush);
        }
    }

    private Partitioner<GenericRecord> buildPartitioner(V sinkConfig) {
        Partitioner<GenericRecord> partitioner;
        String partitionerTypeName = sinkConfig.getPartitionerType();
        PartitionerType partitionerType =
                EnumUtils.getEnumIgnoreCase(PartitionerType.class, partitionerTypeName, PartitionerType.PARTITION);
        switch (partitionerType) {
            case TIME:
                partitioner = new TimePartitioner<>();
                break;
            case PARTITION:
            default:
                partitioner = new SimplePartitioner<>();
                break;
        }
        partitioner.configure(sinkConfig);
        return partitioner;
    }

    private Format<GenericRecord> buildFormat(V sinkConfig) {
        String formatType = StringUtils.defaultIfBlank(sinkConfig.getFormatType(), "json");
        switch (formatType) {
            case "avro":
                return new AvroFormat();
            case "parquet":
                return new ParquetFormat();
            case "json":
                return new JsonFormat();
            case "bytes":
                return new BytesFormat();
            default:
                throw new RuntimeException("not support formatType " + formatType);
        }
    }

    protected abstract V loadConfig(Map<String, Object> config, SinkContext sinkContext) throws IOException;
    protected abstract BlobWriter initBlobWriter(V sinkConfig);

    public void uploadPayload(ByteBuffer payload, String filepath) throws IOException {
        blobWriter.uploadBlob(filepath, payload);
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        flushIfNeeded(true);
        flushExecutor.shutdown();
        if (!flushExecutor.awaitTermination(10 * sinkConfig.getBatchTimeMs(), TimeUnit.MILLISECONDS)) {
            log.error("flushExecutor did not terminate in {} ms", 10 * sinkConfig.getBatchTimeMs());
        }
        blobWriter.close();
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("write record={}.", record);
        }

        if (!isRunning) {
            log.warn("sink is stopped and cannot send the record {}", record);
            record.fail();
            return;
        }

        checkArgument(record.getMessage().isPresent());
        pendingFlushQueue.put(record);
        currentBatchSize.addAndGet(1);
        flushIfNeeded(false);
    }


    private void flush() {
        if (log.isDebugEnabled()) {
            log.debug("flush requested, pending: {}, batchSize: {}",
                currentBatchSize.get(), maxBatchSize);
        }

        if (pendingFlushQueue.isEmpty()) {
            log.info("Skip flushing, because the pending flush queue is empty ...");
            return;
        }

        if (!isFlushRunning.compareAndSet(false, true)) {
            log.info("Skip flushing, because there is an outstanding flush ...");
            return;
        }

        try {
            unsafeFlush();
        } catch (Throwable t) {
            log.error("Caught unexpected exception: ", t);
        } finally {
            isFlushRunning.compareAndSet(true, false);
        }
    }

    private void unsafeFlush() {
        final List<Record<GenericRecord>> recordsToInsert = Lists.newArrayList();
        while (!pendingFlushQueue.isEmpty() && recordsToInsert.size() < maxBatchSize) {
            Record<GenericRecord> r = pendingFlushQueue.poll();
            if (r != null) {
                recordsToInsert.add(r);
            }
        }
        log.info("Flushing {} buffered records to blob store", recordsToInsert.size());
        if (log.isDebugEnabled()) {
            log.debug("buffered records {}", recordsToInsert);
        }

        Record<GenericRecord> firstRecord = recordsToInsert.get(0);
        Schema<GenericRecord> schema = getPulsarSchema(firstRecord);
        if (!format.doSupportPulsarSchemaType(schema.getSchemaInfo().getType())) {
            log.warn("sink does not support schema type {}", schema.getSchemaInfo().getType());
            bulkHandleFailedRecords(recordsToInsert);
            return;
        }

        String filepath = "";
        try {
            format.initSchema(schema);
            final Iterator<Record<GenericRecord>> iter = recordsToInsert.iterator();
            filepath = buildPartitionPath(firstRecord, partitioner, format);
            ByteBuffer payload = bindValue(iter, format);
            log.info("Uploading blob {} currentBatchSize {}", filepath, currentBatchSize.get());
            long elapsedMs = System.currentTimeMillis();
            uploadPayload(payload, filepath);
            elapsedMs = System.currentTimeMillis() - elapsedMs;
            log.debug("Uploading blob elapsed time in ms: {}", elapsedMs);
            recordsToInsert.forEach(Record::ack);
            currentBatchSize.addAndGet(-1 * recordsToInsert.size());
            if (sinkContext != null) {
                sinkContext.recordMetric(METRICS_TOTAL_SUCCESS, recordsToInsert.size());
                sinkContext.recordMetric(METRICS_LATEST_UPLOAD_ELAPSED_TIME, elapsedMs);
            }
            log.info("Successfully uploaded blob {} currentBatchSize {}", filepath, currentBatchSize.get());
        } catch (Exception e) {
            if (e instanceof ContainerNotFoundException) {
                log.error("Blob {} is not found", filepath, e);
            } else if (e instanceof IOException) {
                log.error("Failed to write to blob {}", filepath, e);
            } else if (e instanceof UnsupportedOperationException || e instanceof IllegalArgumentException) {
                log.error("Failed to handle message schema {}", schema, e);
            } else {
                log.error("Encountered unknown error writing to blob {}", filepath, e);
            }
            bulkHandleFailedRecords(recordsToInsert);
        }
    }

    private void bulkHandleFailedRecords(List<Record<GenericRecord>> failedRecords) {
        if (sinkConfig.isSkipFailedMessages()) {
            failedRecords.forEach(Record::ack);
        } else {
            failedRecords.forEach(Record::fail);
        }
        if (sinkContext != null) {
            sinkContext.recordMetric(METRICS_TOTAL_FAILURE, failedRecords.size());
        }
    }

    public ByteBuffer bindValue(Iterator<Record<GenericRecord>> message,
                                Format<GenericRecord> format) throws Exception {
        return format.recordWriterBuf(message);
    }

    public String buildPartitionPath(Record<GenericRecord> message,
                                     Partitioner<GenericRecord> partitioner,
                                     Format<?> format) {

        String encodePartition = partitioner.encodePartition(message, System.currentTimeMillis());
        String partitionedPath = partitioner.generatePartitionedPath(message.getTopicName().get(), encodePartition);
        String path = pathPrefix + partitionedPath + format.getExtension();
        log.info("generate message[recordSequence={}] savePath: {}", message.getRecordSequence().get(), path);
        return path;
    }

}
