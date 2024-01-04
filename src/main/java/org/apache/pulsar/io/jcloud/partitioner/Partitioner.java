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

import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/**
 * The Partitioner interface offers a mechanism to categorize a list of records into distinct parts.
 */
public interface Partitioner {
    /**
     * The partition method takes a list of records and returns a map. Each key in the map represents a
     * unique partition, and the corresponding value is a list of records that belong to that partition.
     *
     * @param records A list of records to be partitioned. Each record is of the type GenericRecord.
     * @return A map where keys represent unique partitions and values are lists of records
     * associated with their respective partitions. The unique partition is consistently used as a file path in the
     * cloud storage system.
     */
    Map<String, List<Record<GenericRecord>>> partition(List<Record<GenericRecord>> records);
}
