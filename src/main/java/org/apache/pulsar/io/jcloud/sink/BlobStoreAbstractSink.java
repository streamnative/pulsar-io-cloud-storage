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
import static org.apache.pulsar.io.jcloud.batch.BatchManager.getBytesSum;
import static org.apache.pulsar.io.jcloud.util.AvroRecordUtil.getPulsarSchema;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.batch.BatchManager;
import org.apache.pulsar.io.jcloud.format.Format;
import org.apache.pulsar.io.jcloud.partitioner.Partitioner;
import org.apache.pulsar.io.jcloud.schema.ResolvedSchemaRecord;
import org.apache.pulsar.io.jcloud.schema.SchemalessJsonRecordNormalizer;
import org.apache.pulsar.io.jcloud.schema.SchemalessJsonRecordNormalizer.MalformedRecordException;
import org.apache.pulsar.io.jcloud.schema.TopicSchemaResolver;
import org.apache.pulsar.io.jcloud.writer.BlobWriter;
import org.jclouds.blobstore.ContainerNotFoundException;

/**
 * A Simple abstract class for BlobStore sink.
 * Users need to implement extractKeyValue function to use this sink
 */
@Slf4j
public abstract class BlobStoreAbstractSink<V extends BlobStoreAbstractConfig> implements Sink<GenericRecord> {

    private static final String METRICS_TOTAL_SUCCESS = "_cloud_storage_sink_total_success_";
    private static final String METRICS_TOTAL_FAILURE = "_cloud_storage_sink_total_failure_";
    private static final String METRICS_LATEST_UPLOAD_ELAPSED_TIME = "_cloud_storage_latest_upload_elapsed_time_";

    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setNameFormat("pulsar-io-cloud-storage-sink-flush-thread")
                    .build());

    protected Partitioner<GenericRecord> partitioner;
    protected Format<GenericRecord> format;
    protected BatchManager batchManager;
    protected BlobWriter blobWriter;
    private V sinkConfig;
    private SinkContext sinkContext;
    private volatile boolean isRunning = false;
    private TopicSchemaResolver topicSchemaResolver;
    private final AtomicBoolean systemFailed = new AtomicBoolean();

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        this.sinkConfig = loadConfig(config, sinkContext);
        this.sinkContext = sinkContext;
        if (sinkConfig.getTopicSchemaFallback().isEnabled()) {
            this.topicSchemaResolver = new TopicSchemaResolver(sinkConfig.getTopicSchemaFallback(), sinkContext);
        }
        this.format = Format.buildFormat(sinkConfig);
        if (topicSchemaResolver != null) {
            topicSchemaResolver.validateFormat(format);
        }
        this.partitioner = Partitioner.buildPartitioner(sinkConfig);
        this.blobWriter = initBlobWriter(sinkConfig);
        this.batchManager = BatchManager.createBatchManager(sinkConfig);
        this.isRunning = true;
        flushExecutor.submit(() -> {
            while (isRunning) {
                try {
                    Map<String, List<Record<GenericRecord>>> recordsToInsertByTopic = batchManager.pollNeedFlushData();
                    if (recordsToInsertByTopic.isEmpty()) {
                        log.debug("Skip flushing because the need flush data is empty...");
                        Thread.sleep(100);
                    }
                    flush(recordsToInsertByTopic);
                } catch (Throwable t) {
                    log.error("Caught unexpected exception: ", t);
                    if (topicSchemaResolver != null) {
                        stopForFailure(t, true);
                    } else {
                        sinkContext.fatal(t);
                    }
                }
            }
        });
    }

    protected abstract V loadConfig(Map<String, Object> config, SinkContext sinkContext) throws IOException;
    protected abstract BlobWriter initBlobWriter(V sinkConfig);

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        if (!isRunning) {
            if (topicSchemaResolver != null) {
                new ResolvedSchemaRecord(record).releaseUnacknowledged();
                return;
            }
            throw new RuntimeException("sink is stopped and cannot send the record");
        }
        checkArgument(record.getMessage().isPresent());
        if (topicSchemaResolver == null) {
            batchManager.add(record);
            return;
        }
        ResolvedSchemaRecord accepted = new ResolvedSchemaRecord(record);
        try {
            SchemalessJsonRecordNormalizer normalizer = topicSchemaResolver.forRecord(record);
            if (normalizer != null) {
                try {
                    accepted = normalizer.normalize(record);
                    metric("_cloud_storage_schemaless_decoded_", 1);
                } catch (MalformedRecordException e) {
                    metric("_cloud_storage_schemaless_malformed_", 1);
                    // Only bounded, static reason codes; never payload, field values, or parser exceptions.
                    log.warn("Schemaless JSON rejected: {}", e.getMessage());
                    if (sinkConfig.getSchemalessDecodeFailureAction()
                            == BlobStoreAbstractConfig.SchemalessDecodeFailureAction.LOG_AND_DROP) {
                        metric("_cloud_storage_schemaless_explicitly_dropped_", 1);
                        complete(accepted, true);
                    } else {
                        metric("_cloud_storage_schemaless_redelivered_", 1);
                        complete(accepted, false);
                    }
                    return;
                }
            }
            batchManager.add(accepted);
        } catch (Exception e) {
            // Do not throw after a callback: the worker would call fail() again on the original record.
            accepted.releaseUnacknowledged();
            if (isRunning) {
                stopForFailure(e, true);
            }
        }
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        if (batchManager != null && topicSchemaResolver != null) {
            batchManager.closeAndDrain().values().forEach(this::releaseUnacknowledged);
        }
        flushExecutor.shutdown();
        if (!flushExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
            // Never close the writer while upload is still using it.
            throw new IllegalStateException("flushExecutor did not terminate in 30 seconds");
        }
        if (blobWriter != null) {
            blobWriter.close();
        }
    }

    void flush(Map<String, List<Record<GenericRecord>>> recordsToInsertByTopic) {
        try {
            flushGroups(recordsToInsertByTopic);
        } finally {
            if (topicSchemaResolver != null) {
                recordsToInsertByTopic.values().forEach(this::releaseUnacknowledged);
            }
        }
    }

    private void flushGroups(Map<String, List<Record<GenericRecord>>> recordsToInsertByTopic) {
        final long timeStampForPartitioning = System.currentTimeMillis();
        for (Map.Entry<String, List<Record<GenericRecord>>> entry : recordsToInsertByTopic.entrySet()) {
            String topicName = entry.getKey();
            Map<String, List<Record<GenericRecord>>> schemaVersionMap = new LinkedHashMap<>();
            for (Record<GenericRecord> record : entry.getValue()) {
                try {
                    String key = Arrays.toString(record.getValue().getSchemaVersion());
                    if (record instanceof ResolvedSchemaRecord && ((ResolvedSchemaRecord) record).isResolved()) {
                        key = ((ResolvedSchemaRecord) record).getSchemaIdentity();
                    } else if (topicSchemaResolver != null) {
                        Schema<GenericRecord> schema = getPulsarSchema(record);
                        key = "typed:" + key + ":" + schema.getSchemaInfo().getType() + ":"
                                + schema.getSchemaInfo().getSchemaDefinition();
                    }
                    schemaVersionMap.computeIfAbsent(key, ignored -> new ArrayList<>()).add(record);
                } catch (Exception e) {
                    bulkHandleFailedRecords(e, List.of(record));
                }
            }
            for (List<Record<GenericRecord>> singleTopicRecordsToInsert : schemaVersionMap.values()) {
                Record<GenericRecord> firstRecord = singleTopicRecordsToInsert.get(0);
                Schema<GenericRecord> schema;
                try {
                    schema = getPulsarSchema(firstRecord);
                    if (schema == null || schema.getSchemaInfo() == null || schema.getSchemaInfo().getType() == null) {
                        throw new IllegalArgumentException("Message schema is unavailable");
                    }
                } catch (Exception e) {
                    log.error("Failed to retrieve message schema", e);
                    bulkHandleFailedRecords(e, singleTopicRecordsToInsert);
                    continue;
                }

                if (!format.doSupportPulsarSchemaType(schema.getSchemaInfo().getType())) {
                    String errorMsg = "Sink does not support schema type of pulsar: "
                            + schema.getSchemaInfo().getType();
                    log.error(errorMsg);
                    bulkHandleFailedRecords(new UnsupportedOperationException(errorMsg), singleTopicRecordsToInsert);
                    continue;
                }

                String filepath = "";
                int uploadSize = singleTopicRecordsToInsert.size();
                long uploadBytes;
                long elapsedMs;
                try {
                    format.initSchema(schema);
                    final Iterator<Record<GenericRecord>> iter = singleTopicRecordsToInsert.iterator();
                    filepath = buildPartitionPath(firstRecord, partitioner, format, timeStampForPartitioning);
                    ByteBuffer payload = bindValue(iter, format);
                    uploadBytes = getBytesSum(singleTopicRecordsToInsert);
                    log.info("Uploading blob {} from topic {} uploadSize:{} uploadBytes:{} currentBatchStatus:{}",
                            filepath, topicName, uploadSize, uploadBytes, batchManager.getCurrentStatsStr());
                    elapsedMs = System.currentTimeMillis();
                    upload(filepath, payload);
                    elapsedMs = System.currentTimeMillis() - elapsedMs;
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
                    bulkHandleFailedRecords(e, singleTopicRecordsToInsert);
                    continue;
                }
                // Callbacks and telemetry cannot re-enter the upload failure path.
                singleTopicRecordsToInsert.forEach(r -> complete(r, true));
                metric(METRICS_TOTAL_SUCCESS, singleTopicRecordsToInsert.size());
                metric(METRICS_LATEST_UPLOAD_ELAPSED_TIME, elapsedMs);
                log.info("Successfully uploaded blob {} from topic {} uploadSize {} uploadBytes {}",
                        filepath, topicName, uploadSize, uploadBytes);
            }
        }
    }

    private void bulkHandleFailedRecords(Throwable t, List<Record<GenericRecord>> failedRecords) {
        if (topicSchemaResolver != null) {
            releaseUnacknowledged(failedRecords);
            stopForFailure(t, !(t instanceof UploadFailureException));
        } else if (sinkConfig.isSkipFailedMessages()) {
            failedRecords.forEach(r -> complete(r, true));
        } else {
            try {
                sinkContext.fatal(t);
            } catch (RuntimeException e) {
                log.error("Unable to notify runtime of batch failure", e);
            } finally {
                failedRecords.forEach(r -> complete(r, false));
            }
        }
        metric(METRICS_TOTAL_FAILURE, failedRecords.size());
    }

    private void upload(String filepath, ByteBuffer payload) throws Exception {
        for (int attempt = 0; ; attempt++) {
            try {
                blobWriter.uploadBlob(filepath, payload.duplicate());
                return;
            } catch (Exception e) {
                metric("_cloud_storage_upload_failure_", 1);
                if (topicSchemaResolver == null || attempt == 2 || !isRunning) {
                    if (topicSchemaResolver == null) {
                        throw e;
                    }
                    throw new UploadFailureException(e);
                }
                TimeUnit.MILLISECONDS.sleep(100L << attempt);
            }
        }
    }

    private static final class UploadFailureException extends Exception {
        private static final long serialVersionUID = 1L;

        private UploadFailureException(Exception cause) {
            super("Upload attempts exhausted; input paused until explicit recovery", cause);
        }
    }

    private void metric(String name, double value) {
        if (sinkContext != null) {
            try {
                sinkContext.recordMetric(name, value);
            } catch (RuntimeException e) {
                log.warn("Cloud storage metric reporting failed");
            }
        }
    }

    private void complete(Record<GenericRecord> record, boolean ack) {
        try {
            if (ack) {
                record.ack();
            } else {
                record.fail();
            }
        } catch (RuntimeException e) {
            // The callback may already have released a pooled message. Never call it a second time.
            log.error("Cloud storage record completion failed", e);
            stopForFailure(e, true);
        }
    }

    private void releaseUnacknowledged(List<Record<GenericRecord>> records) {
        for (Record<GenericRecord> record : records) {
            if (record instanceof ResolvedSchemaRecord) {
                try {
                    ((ResolvedSchemaRecord) record).releaseUnacknowledged();
                } catch (RuntimeException e) {
                    log.error("Unable to release unacknowledged record", e);
                }
            }
        }
    }

    private void stopForFailure(Throwable failure, boolean fatal) {
        isRunning = false;
        if (!systemFailed.compareAndSet(false, true)) {
            return;
        }
        if (topicSchemaResolver != null && batchManager != null) {
            batchManager.closeAndDrain().values().forEach(this::releaseUnacknowledged);
        }
        // Backpressure stops consumption. Do not negative-ACK healthy messages into a DLQ during an outage.
        if (sinkContext != null) {
            try {
                pauseInputConsumers(sinkContext, sinkConfig.getTopicSchemaFallback().getLookupTimeoutMs());
                metric("_cloud_storage_sink_paused_", 1);
                log.error("Cloud storage input partitions paused; explicit recovery is required (fatal={})", fatal);
            } catch (Exception e) {
                metric("_cloud_storage_sink_pause_failure_", 1);
                log.error("Unable to pause all sink input partitions; reporting fatal system failure", e);
                failure.addSuppressed(e);
                fatal = true;
            }
            if (fatal) {
                try {
                    sinkContext.fatal(failure);
                } catch (RuntimeException e) {
                    log.error("Unable to notify runtime of system failure", e);
                }
            }
        }
    }

    static void pauseInputConsumers(SinkContext context, long timeoutMs) throws Exception {
        IllegalStateException failure = new IllegalStateException("Unable to pause all sink input partitions");
        for (String topic : context.getInputTopics()) {
            List<String> partitions;
            try {
                partitions = context.getPulsarClient().getPartitionsForTopic(topic, false)
                        .get(timeoutMs, TimeUnit.MILLISECONDS);
                if (partitions.isEmpty()) {
                    throw new IllegalStateException("No partitions found for sink input " + topic);
                }
            } catch (Exception e) {
                failure.addSuppressed(e);
                continue;
            }
            for (String partition : partitions) {
                try {
                    TopicName name = TopicName.get(partition);
                    // ContextImpl looks up a single consumer; -1 is only valid for a non-partitioned topic.
                    context.pause(name.getPartitionedTopicName(), name.getPartitionIndex());
                } catch (Exception e) {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure.getSuppressed().length != 0) {
            throw failure;
        }
    }

    public ByteBuffer bindValue(Iterator<Record<GenericRecord>> message,
                                Format<GenericRecord> format) throws Exception {
        return format.recordWriterBuf(message);
    }

    public String buildPartitionPath(Record<GenericRecord> message,
                                     Partitioner<GenericRecord> partitioner,
                                     Format<?> format,
                                     long partitioningTimestamp) {

        String encodePartition = partitioner.encodePartition(message, partitioningTimestamp);
        String partitionedPath = partitioner.generatePartitionedPath(message.getTopicName().get(), encodePartition);
        // A replay can have different membership after partial ACK or a pin change. Never overwrite its old batch.
        String suffix = topicSchemaResolver != null ? "-" + UUID.randomUUID() : "";
        String path = sinkConfig.getPathPrefix() + partitionedPath + suffix + format.getExtension();
        log.info("generate message[messageId={}] savePath: {}", message.getMessage().get().getMessageId(), path);
        return path;
    }
}
