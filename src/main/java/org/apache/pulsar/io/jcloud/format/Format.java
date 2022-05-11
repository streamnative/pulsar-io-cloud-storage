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
package org.apache.pulsar.io.jcloud.format;

import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.jcloud.shade.com.google.common.io.ByteSource;

/**
 * record format.
 */
public interface Format<T> {
    /**
     * get format extension.
     *
     * @return format extension
     */
    String getExtension();

    void initSchema(Schema<T> schema);

    boolean doSupportPulsarSchemaType(SchemaType schemaType);

    /**
     * format record to bytes.
     *
     * @param record record
     * @return bytes warp
     * @throws Exception exception
     */
    default ByteSource recordWriter(Iterator<Record<T>> record) throws Exception {
        return ByteSource.wrap(recordWriterBuf(record).array());
    }

    ByteBuffer recordWriterBuf(Iterator<Record<T>> record) throws Exception;
}
