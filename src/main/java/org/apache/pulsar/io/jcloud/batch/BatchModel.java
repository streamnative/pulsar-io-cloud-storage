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

/**
 * Enum representing the different batch models.
 */
public enum BatchModel {
    /**
     * BlendBatchManager is a type of BatchManager that uses a single BatchContainer
     * for all topics. This means that all records, regardless of topic, are batched together.
     */
    BLEND,
    /**
     * PartitionedBatchManager is a type of BatchManager that uses separate BatchContainers
     * for each topic. This means that records are batched separately for each topic.
     * Note: When set to PARTITIONED, the connector will cache data up to the size of the
     *   number of subscribed topics multiplied by maxBatchBytes. This means you need to anticipate the connector
     *   memory requirements in advance.
     */
    PARTITIONED
}
