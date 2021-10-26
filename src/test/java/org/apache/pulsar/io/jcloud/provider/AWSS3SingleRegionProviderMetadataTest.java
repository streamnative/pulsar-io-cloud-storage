package org.apache.pulsar.io.jcloud.provider;

import static org.jclouds.location.reference.LocationConstants.PROPERTY_REGIONS;
import static org.junit.Assert.assertEquals;
import org.jclouds.providers.ProviderMetadata;
import org.junit.Test;

public class AWSS3SingleRegionProviderMetadataTest {
    @Test
    public void testSingleRegionMetadata() {
        String regionName = "cn-north-1";
        String endpointURL = "";
        ProviderMetadata metadata = new AWSS3SingleRegionProviderMetadata(regionName, endpointURL);
        assertEquals(metadata.getDefaultProperties().getProperty(PROPERTY_REGIONS), regionName);
    }
}
