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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * BatchContainer is used to store and manage batches of records.
 * It keeps track of the current batch size and bytes, and checks
 * if the batch needs to be flushed based on the max batch size, bytes, and time.
 */
public class BatchContainer {

    private final long maxBatchSize;
    private final long maxBatchBytes;
    private final long maxBatchTimeMs;
    private final AtomicLong currentBatchSize = new AtomicLong(0L);
    private final AtomicLong currentBatchBytes = new AtomicLong(0L);
    private volatile long lastPollRecordsTime;
    private final List<Record<GenericRecord>> pendingFlushList;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();

    public BatchContainer(long maxBatchSize, long maxBatchBytes, long maxBatchTimeMs) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchBytes = maxBatchBytes;
        this.maxBatchTimeMs = maxBatchTimeMs;
        this.lastPollRecordsTime = System.currentTimeMillis();
        this.pendingFlushList = new ArrayList<>((int) maxBatchSize);
    }

    public void add(Record<GenericRecord> record) throws InterruptedException {
        lock.lock();
        try {
            // Allow exceeding the maximum value once
            long recordSize = record.getMessage().get().size();
            pendingFlushList.add(record);
            currentBatchSize.incrementAndGet();
            currentBatchBytes.addAndGet(recordSize);

            // Wait if the batch needs to be flushed
            while (needFlush()) {
                notFull.await();
            }
        } finally {
            lock.unlock();
        }
    }

    public long getCurrentBatchSize() {
        return currentBatchSize.get();
    }

    public long getCurrentBatchBytes() {
        return currentBatchBytes.get();
    }

    public List<Record<GenericRecord>> pollNeedFlushRecords() {
        if (currentBatchSize.get() == 0) {
            return Collections.emptyList();
        }
        lock.lock();
        try {
            if (!needFlush()) {
                return Collections.emptyList();
            }
            List<Record<GenericRecord>> needFlushRecords = new ArrayList<>(pendingFlushList);
            pendingFlushList.clear();
            // Clear the pending list
            currentBatchSize.set(0);
            currentBatchBytes.set(0);
            lastPollRecordsTime = System.currentTimeMillis();
            return needFlushRecords;
        } finally {
            notFull.signalAll();
            lock.unlock();
        }
    }

    private boolean needFlush() {
        long currentTime = System.currentTimeMillis();
        return currentBatchSize.get() >= maxBatchSize
                || currentBatchBytes.get() >= maxBatchBytes
                || (currentTime - lastPollRecordsTime) >= maxBatchTimeMs;
    }
}
