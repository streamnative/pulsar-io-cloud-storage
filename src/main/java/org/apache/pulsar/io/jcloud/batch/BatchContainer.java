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
package org.apache.pulsar.io.jcloud.batch;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * BatchContainer is used to store and manage batches of records.
 * It keeps track of the current batch size and bytes, and checks
 * if the batch needs to be flushed based on the max batch size and bytes.
 */
public class BatchContainer {

    private final long maxBatchSize;
    private final long maxBatchBytes;
    private final AtomicLong currentBatchSize = new AtomicLong(0L);
    private final AtomicLong currentBatchBytes = new AtomicLong(0L);
    private ArrayBlockingQueue<Record<GenericRecord>> pendingFlushQueue;

    public BatchContainer(long maxBatchSize, long maxBatchBytes, int maxPendingQueueSize) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchBytes = maxBatchBytes;
        this.pendingFlushQueue = new ArrayBlockingQueue<>(maxPendingQueueSize);
    }

    public void add(Record<GenericRecord> record) throws InterruptedException {
        pendingFlushQueue.put(record);
        updateCurrentBatchSize(1);
        updateCurrentBatchBytes(record.getMessage().get().size());
    }

    public long getCurrentBatchSize() {
        return currentBatchSize.get();
    }

    public long getCurrentBatchBytes() {
        return currentBatchBytes.get();
    }

    public void updateCurrentBatchSize(long delta) {
        currentBatchSize.addAndGet(delta);
    }

    public void updateCurrentBatchBytes(long delta) {
        currentBatchBytes.addAndGet(delta);
    }

    public boolean isEmpty() {
        return pendingFlushQueue.isEmpty();
    }

    public boolean needFlush() {
        return currentBatchSize.get() >= maxBatchSize || currentBatchBytes.get() >= maxBatchBytes;
    }

    public List<Record<GenericRecord>> poolNeedFlushRecords() {
        final List<Record<GenericRecord>> needFlushRecords = Lists.newArrayList();
        long recordsToInsertBytes = 0;
        while (!pendingFlushQueue.isEmpty() && needFlushRecords.size() < maxBatchSize
                && recordsToInsertBytes < maxBatchBytes) {
            Record<GenericRecord> r = pendingFlushQueue.poll();
            if (r != null) {
                if (r.getMessage().isPresent()) {
                    long recordBytes = r.getMessage().get().size();
                    recordsToInsertBytes += recordBytes;
                }
                needFlushRecords.add(r);
            }
        }
        updateCurrentBatchBytes(-1 * recordsToInsertBytes);
        updateCurrentBatchSize(-1 * needFlushRecords.size());
        return needFlushRecords;
    }

}
