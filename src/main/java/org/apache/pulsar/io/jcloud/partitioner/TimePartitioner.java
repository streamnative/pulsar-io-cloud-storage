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
package org.apache.pulsar.io.jcloud.partitioner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * The TimePartitioner is used to partition records based on the current sink timestamp.
 */
public class TimePartitioner implements Partitioner {
    /**
     * Partitions a list of records into a map, where the key is the current system time in milliseconds
     * and the value is the list of records.
     *
     * @param records A list of records of type GenericRecord that need to be partitioned.
     * @return A map where the key is the current system time in milliseconds and the value is the list of records.
     */
    @Override
    public Map<String, List<Record<GenericRecord>>> partition(List<Record<GenericRecord>> records) {
        return Collections.singletonMap(Long.toString(System.currentTimeMillis()), records);
    }
}
