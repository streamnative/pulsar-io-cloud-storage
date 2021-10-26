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
package org.apache.pulsar.io.jcloud.provider;

import static org.jclouds.location.reference.LocationConstants.PROPERTY_REGIONS;
import static org.junit.Assert.assertEquals;
import org.jclouds.providers.ProviderMetadata;
import org.junit.Test;

/**
 * AWSS3SingleRegionProviderMetadata unit test.
 */
public class AWSS3SingleRegionProviderMetadataTest {
    @Test
    public void testSingleRegionMetadata() {
        String regionName = "cn-north-1";
        String endpointURL = "";
        ProviderMetadata metadata = new AWSS3SingleRegionProviderMetadata(regionName, endpointURL);
        assertEquals(metadata.getDefaultProperties().getProperty(PROPERTY_REGIONS), regionName);
    }
}
