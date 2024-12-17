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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.apache.pulsar.io.jcloud.batch.BatchManager;
import org.apache.pulsar.io.jcloud.format.Format;
import org.apache.pulsar.io.jcloud.partitioner.Partitioner;
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

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        this.sinkConfig = loadConfig(config, sinkContext);
        this.sinkContext = sinkContext;
        this.format = Format.buildFormat(sinkConfig);
        this.partitioner = Partitioner.buildPartitioner(sinkConfig);
        this.isRunning = true;
        this.blobWriter = initBlobWriter(sinkConfig);
        this.batchManager = BatchManager.createBatchManager(sinkConfig);
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
                    sinkContext.fatal(t);
                }
            }
        });
    }

    protected abstract V loadConfig(Map<String, Object> config, SinkContext sinkContext) throws IOException;
    protected abstract BlobWriter initBlobWriter(V sinkConfig);

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        if (!isRunning) {
            throw new RuntimeException("sink is stopped and cannot send the record");
        }
        checkArgument(record.getMessage().isPresent());
        batchManager.add(record);
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        flushExecutor.shutdown();
        if (!flushExecutor.awaitTermination(10 * sinkConfig.getBatchTimeMs(), TimeUnit.MILLISECONDS)) {
            log.error("flushExecutor did not terminate in {} ms", 10 * sinkConfig.getBatchTimeMs());
        }
        blobWriter.close();
    }

    private void flush(Map<String, List<Record<GenericRecord>>> recordsToInsertByTopic) {
        final long timeStampForPartitioning = System.currentTimeMillis();
        for (Map.Entry<String, List<Record<GenericRecord>>> entry : recordsToInsertByTopic.entrySet()) {
            String topicName = entry.getKey();
            List<Record<GenericRecord>> singleTopicRecordsToInsert = entry.getValue();
            Record<GenericRecord> firstRecord = singleTopicRecordsToInsert.get(0);
            Schema<GenericRecord> schema;
            try {
                schema = getPulsarSchema(firstRecord);
            } catch (Exception e) {
                log.error("Failed to retrieve message schema", e);
                bulkHandleFailedRecords(e, singleTopicRecordsToInsert);
                return;
            }

            if (!format.doSupportPulsarSchemaType(schema.getSchemaInfo().getType())) {
                String errorMsg = "Sink does not support schema type of pulsar: " + schema.getSchemaInfo().getType();
                log.error(errorMsg);
                bulkHandleFailedRecords(new UnsupportedOperationException(errorMsg), singleTopicRecordsToInsert);
                return;
            }

            String filepath = "";
            try {
                format.initSchema(schema);
                final Iterator<Record<GenericRecord>> iter = singleTopicRecordsToInsert.iterator();
                filepath = buildPartitionPath(firstRecord, partitioner, format, timeStampForPartitioning);
                ByteBuffer payload = bindValue(iter, format);
                int uploadSize = singleTopicRecordsToInsert.size();
                long uploadBytes = getBytesSum(singleTopicRecordsToInsert);
                log.info("Uploading blob {} from topic {} uploadSize:{} uploadBytes:{} currentBatchStatus:{}",
                        filepath, topicName, uploadSize, uploadBytes, batchManager.getCurrentStatsStr());
                long elapsedMs = System.currentTimeMillis();
                blobWriter.uploadBlob(filepath, payload);
                elapsedMs = System.currentTimeMillis() - elapsedMs;
                log.debug("Uploading blob {} elapsed time in ms: {}", filepath, elapsedMs);
                singleTopicRecordsToInsert.forEach(Record::ack);
                if (sinkContext != null) {
                    sinkContext.recordMetric(METRICS_TOTAL_SUCCESS, singleTopicRecordsToInsert.size());
                    sinkContext.recordMetric(METRICS_LATEST_UPLOAD_ELAPSED_TIME, elapsedMs);
                }
                log.info("Successfully uploaded blob {} from topic {} uploadSize {} uploadBytes {}",
                    filepath, entry.getKey(),
                    uploadSize, uploadBytes);
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
            }
        }
    }

    private void bulkHandleFailedRecords(Throwable t, List<Record<GenericRecord>> failedRecords) {
        if (sinkConfig.isSkipFailedMessages()) {
            failedRecords.forEach(Record::ack);
        } else {
            sinkContext.fatal(t);
            failedRecords.forEach(Record::fail);
        }
        sinkContext.recordMetric(METRICS_TOTAL_FAILURE, failedRecords.size());
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
        String path = sinkConfig.getPathPrefix() + partitionedPath + format.getExtension();
        log.info("generate message[messageId={}] savePath: {}", message.getMessage().get().getMessageId(), path);
        return path;
    }
}
