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
package org.apache.pulsar.ecosystem.io.s3.sink;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.s3.BlobStoreAbstractConfig;
import org.apache.pulsar.ecosystem.io.s3.format.AvroFormat;
import org.apache.pulsar.ecosystem.io.s3.format.Format;
import org.apache.pulsar.ecosystem.io.s3.format.JsonFormat;
import org.apache.pulsar.ecosystem.io.s3.format.ParquetFormat;
import org.apache.pulsar.ecosystem.io.s3.partitioner.Partitioner;
import org.apache.pulsar.ecosystem.io.s3.partitioner.SimplePartitioner;
import org.apache.pulsar.ecosystem.io.s3.partitioner.TimePartitioner;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.PutOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Simple abstract class for BlobStore sink.
 * Users need to implement extractKeyValue function to use this sink
 */
@Slf4j
public abstract class BlobStoreAbstractSink<V extends BlobStoreAbstractConfig> implements Sink<GenericRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlobStoreAbstractSink.class);

    private V sinkConfig;

    protected BlobStoreContext context;
    protected BlobStore blobStore;

    protected Partitioner<GenericRecord> partitioner;

    protected Format<V, Record<GenericRecord>> format;

    private List<Pair<Record<GenericRecord>, Blob>> incomingList;

    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        sinkConfig = loadConfig(config, sinkContext);

        checkNotNull(sinkConfig.getBucket(), "bucket property not set.");
        checkNotNull(sinkConfig.getRegion(), "region property not set.");
        context = buildBlobStoreContext(sinkConfig);
        blobStore = context.getBlobStore();
        boolean testCase = "transient".equalsIgnoreCase(sinkConfig.getProvider());
        if (!blobStore.containerExists(sinkConfig.getBucket()) && testCase) {
            //test use
            blobStore.createContainerInLocation(null, sinkConfig.getBucket());
        }
        checkArgument(blobStore.containerExists(sinkConfig.getBucket()), "%s bucket not exist", sinkConfig.getBucket());
        String formatType = StringUtils.defaultIfBlank(sinkConfig.getFormatType(), "avro");
        switch (formatType) {
            case "avro":
                format = new AvroFormat<>();
                break;
            case "json":
                format = new JsonFormat<>();
                break;
            case "parquet":
                format = new ParquetFormat<>();
                break;
            default:
                format = new JsonFormat<>();
        }
        switch (sinkConfig.getPartitionerType()) {
            case "time":
                partitioner = new TimePartitioner<>();
                break;
            case "partition":
            default:
                partitioner = new SimplePartitioner<>();
        }
        partitioner.configure(sinkConfig);

        long batchTimeMs = 1000;
        incomingList = Lists.newArrayList();
        flushExecutor = Executors.newScheduledThreadPool(1);
        flushExecutor.scheduleAtFixedRate(() -> flush(),
                batchTimeMs, batchTimeMs, TimeUnit.MILLISECONDS);
    }

    protected abstract V loadConfig(Map<String, Object> config, SinkContext sinkContext) throws IOException;

    protected abstract BlobStoreContext buildBlobStoreContext(V sinkConfig);

    @Override
    public void close() throws Exception {
        if (null != context) {
            context.close();
        }
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {

        LOGGER.info("write message[recordSequence={}]", record.getRecordSequence().get());
        String filepath = buildPartitionPath(record, partitioner, format);

        ByteSource payload = bindValue(record, format);

        Blob blob = blobStore.blobBuilder(filepath)
                .payload(payload)
                .contentLength(payload.size())
                .build();

        int currentSize;

        synchronized (this) {
            incomingList.add(Pair.of(record, blob));
            currentSize = incomingList.size();
        }
        LOGGER.info("build blob success[recordSequence={}]", record.getRecordSequence().get());
        if (currentSize == sinkConfig.getBatchSize()) {
            flushExecutor.submit(() -> flush());
        }
    }

    private void flush() {
        final List<Pair<Record<GenericRecord>, Blob>> recordsToInsert;

        synchronized (this) {
            if (incomingList.isEmpty()) {
                return;
            }

            recordsToInsert = incomingList;
            incomingList = Lists.newArrayList();
        }

        final Iterator<Pair<Record<GenericRecord>, Blob>> iter = recordsToInsert.iterator();

        while (iter.hasNext()) {
            final Pair<Record<GenericRecord>, Blob> blobPair = iter.next();
            try {
                log.info("upload blob {}", blobPair.getLeft().getRecordSequence().get());
                blobStore.putBlob(sinkConfig.getBucket(), blobPair.getRight(), PutOptions.NONE);
                blobPair.getLeft().ack();
                log.info("write success {}", blobPair.getLeft().getRecordSequence().get());
            } catch (ContainerNotFoundException e) {
                log.error("Bad message", e);
                blobPair.getLeft().fail();
                iter.remove();
            } catch (Exception e){
                log.error("write message failed", e);
            }
        }
    }


    public ByteSource bindValue(Record<GenericRecord> message,
                                Format<V, Record<GenericRecord>> format) throws Exception {
        return format.recordWriter(sinkConfig, message);
    }

    public String buildPartitionPath(Record<GenericRecord> message,
                                     Partitioner<GenericRecord> partitioner,
                                     Format<?, Record<GenericRecord>> format) throws Exception {
        String encodePartition = partitioner.encodePartition(message, System.currentTimeMillis());
        String partitionedPath = partitioner.generatePartitionedPath(message.getTopicName().get(), encodePartition);
        String path = partitionedPath + format.getExtension();
        LOGGER.info("write message[recordSequence={}] savePath: {}", message.getRecordSequence().get(), path);

        return path;
    }

}
