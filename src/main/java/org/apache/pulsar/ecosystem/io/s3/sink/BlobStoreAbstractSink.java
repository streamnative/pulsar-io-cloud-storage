/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.s3.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.ecosystem.io.s3.BlobStoreAbstractConfig;
import org.apache.pulsar.ecosystem.io.s3.format.AvroFormat;
import org.apache.pulsar.ecosystem.io.s3.format.Format;
import org.apache.pulsar.ecosystem.io.s3.partitioner.Partitioner;
import org.apache.pulsar.ecosystem.io.s3.partitioner.SimplePartitioner;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.PutOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A Simple abstract class for Hbase sink
 * Users need to implement extractKeyValue function to use this sink
 */
@Slf4j
public abstract class BlobStoreAbstractSink<C extends BlobStoreAbstractConfig,T> implements Sink<T> {

    private C sinkConfig;

    protected BlobStoreContext context;
    protected BlobStore blobStore;

    protected Partitioner<T> partitioner;

    protected Format<C, Record<T>> format;

    private List<Pair<Record<T>, Blob>> incomingList;

    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        sinkConfig = loadConfig(config, sinkContext);

        Preconditions.checkNotNull(sinkConfig.getBucket(), "bucket property not set.");
        Preconditions.checkNotNull(sinkConfig.getRegion(), "region property not set.");
        context = buildBlobStoreContext(sinkConfig);
        blobStore = context.getBlobStore();
        Preconditions.checkArgument(blobStore.containerExists(sinkConfig.getBucket()), "bucket not exist");
        if (sinkConfig.getFormatType()==null) {
            format = new AvroFormat<>();
        }

        if (sinkConfig.getPartitionerType()==null) {
            partitioner = new SimplePartitioner<>();
            partitioner.configure(sinkConfig);
        }
        long batchTimeMs = 1000;
        incomingList = Lists.newArrayList();
        flushExecutor = Executors.newScheduledThreadPool(1);
        flushExecutor.scheduleAtFixedRate(() -> flush(),
                batchTimeMs, batchTimeMs, TimeUnit.MILLISECONDS);
    }

    protected abstract C loadConfig(Map<String, Object> config, SinkContext sinkContext) throws IOException;

    protected abstract BlobStoreContext buildBlobStoreContext(C sinkConfig);

    @Override
    public void close() throws Exception {
        if (null != context) {
            context.close();
        }
    }

    @Override
    public void write(Record<T> record) throws Exception {

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

        if (currentSize == sinkConfig.getBatchSize()) {
            flushExecutor.submit(() -> flush());
        }
    }

    private void flush() {
        final List<Pair<Record<T>, Blob>> recordsToInsert;

        synchronized (this) {
            if (incomingList.isEmpty()) {
                return;
            }

            recordsToInsert = incomingList;
            incomingList = Lists.newArrayList();
        }

        final Iterator<Pair<Record<T>, Blob>> iter = recordsToInsert.iterator();

        while (iter.hasNext()) {
            final Pair<Record<T>, Blob> blobPair = iter.next();

            try {
                blobStore.putBlob(sinkConfig.getBucket(), blobPair.getRight(), PutOptions.NONE);
            } catch (ContainerNotFoundException e) {
                log.error("Bad message", e);
                blobPair.getLeft().fail();
                iter.remove();
            }
        }
    }


    public ByteSource bindValue(Record<T> message, Format<C,Record<T>> format) throws Exception {
        return format.recordWriter(sinkConfig,message);
    }

    public String buildPartitionPath(Record<T> message, Partitioner<T> partitioner, Format<?, ?> format)
            throws Exception {
        String encodePartition = partitioner.encodePartition(message, System.currentTimeMillis());
        String partitionedPath = partitioner.generatePartitionedPath(message.getTopicName().get(), encodePartition);
        return partitionedPath + format.getExtension();
    }

}
