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
package org.apache.pulsar.io.jcloud.schema;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.schema.SchemaInfo;

/** Isolates the Pulsar 4.2.1.1 implementation API, using the framework client's authentication. */
public final class PinnedSchemaLookup {
    private PinnedSchemaLookup() {
    }

    public static SchemaInfo lookup(PulsarClient client, String topic, long version, long timeoutMs) throws Exception {
        // Implementation classes may live in a different NAR classloader. Only the provider API is shared.
        Method cacheAccessor = client.getClass().getMethod("getSchemaProviderLoadingCache");
        Object cache = cacheAccessor.invoke(client);
        SchemaInfoProvider provider = (SchemaInfoProvider) cacheAccessor.getReturnType()
                .getMethod("get", Object.class).invoke(cache, topic);
        byte[] versionBytes = ByteBuffer.allocate(Long.BYTES).putLong(version).array();
        SchemaInfo info = provider.getSchemaByVersion(versionBytes).get(timeoutMs, TimeUnit.MILLISECONDS);
        if (info == null) {
            throw new IllegalArgumentException("Pinned topic schema version does not exist");
        }
        return info;
    }
}
