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

import static org.jclouds.location.reference.LocationConstants.ENDPOINT;
import static org.jclouds.location.reference.LocationConstants.PROPERTY_REGION;
import static org.jclouds.location.reference.LocationConstants.PROPERTY_REGIONS;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import java.net.URI;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jclouds.aws.s3.AWSS3ApiMetadata;
import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.internal.BaseProviderMetadata;

/**
 * A Single Region Provider for jclouds s3.
 * This Provider will overwrite the PROPERTY_REGIONS to have a single supported region
 * and prevent any request to `GetBucketLocation`
 *
 */
@Slf4j
public class AWSS3SingleRegionProviderMetadata extends BaseProviderMetadata {
    public static AWSS3SingleRegionProviderMetadata.Builder builder(String regionName, String endpoint) {
        Region region = RegionUtils.getRegion(regionName);
        return new AWSS3SingleRegionProviderMetadata.Builder(region, endpoint);
    }

    public static AWSS3SingleRegionProviderMetadata.Builder builder() {
        return new AWSS3SingleRegionProviderMetadata.Builder();
    }

    public AWSS3SingleRegionProviderMetadata.Builder toBuilder() {
        return builder().fromProviderMetadata(this);
    }

    public AWSS3SingleRegionProviderMetadata(Builder builder) {
        super(builder);
    }

    public AWSS3SingleRegionProviderMetadata() {
        super(builder());
    }

    public AWSS3SingleRegionProviderMetadata(String regionName, String endpoint) {
        super(builder(regionName, endpoint));
    }

    public static Properties defaultProperties(Region region, String endpoint) {
        Properties properties = AWSS3ProviderMetadata.defaultProperties();
        if (region != null) {
            properties.setProperty(PROPERTY_REGIONS, region.getName());
            if (StringUtils.isNotEmpty(endpoint)) {
                properties.setProperty(PROPERTY_REGION + "." + region.getName() + "." + ENDPOINT,
                        endpoint);
                properties.setProperty("jclouds.endpoint", endpoint);
            }
            log.info("awss3 region properties {} {}:{}", properties.getProperty(PROPERTY_REGIONS),
                    PROPERTY_REGION + "." + region.getName() + "." + ENDPOINT,
                    properties.getProperty(PROPERTY_REGION + "." + region.getName() + "." + ENDPOINT));
            log.info("properties {}", properties);
        }
        return properties;
    }

    /**
     * Builder for AWSS3SingleRegionProviderMetadata.
     *
     */
    public static class Builder extends org.jclouds.providers.internal.BaseProviderMetadata.Builder {
        public Builder(Region region, String endpoint) {
            this.id("aws-s3").name("Amazon Simple Storage Service (S3)")
                    .apiMetadata(new AWSS3ApiMetadata())
                    .homepage(URI.create("http://aws.amazon.com/s3"))
                    .console(URI.create("https://console.aws.amazon.com/s3/home"))
                    .linkedServices("aws-ec2", "aws-elb", "aws-cloudwatch", "aws-s3", "aws-simpledb")
                    .iso3166Codes("US", "US-OH", "US-CA", "US-OR", "CA", "BR-SP", "IE", "GB-LND", "FR-IDF", "DE-HE",
                            "SE-AB", "SG", "AU-NSW", "IN-MH", "JP-13", "KR-11", "CN-BJ", "CN-NX", "BH")
                    .defaultProperties(AWSS3SingleRegionProviderMetadata.defaultProperties(region, endpoint));
        }

        protected Builder() {
            this.id("aws-s3").name("Amazon Simple Storage Service (S3)")
                    .apiMetadata(new AWSS3ApiMetadata())
                    .homepage(URI.create("http://aws.amazon.com/s3"))
                    .console(URI.create("https://console.aws.amazon.com/s3/home"))
                    .linkedServices("aws-ec2", "aws-elb", "aws-cloudwatch", "aws-s3", "aws-simpledb")
                    .iso3166Codes("US", "US-OH", "US-CA", "US-OR", "CA", "BR-SP", "IE", "GB-LND", "FR-IDF", "DE-HE",
                            "SE-AB", "SG", "AU-NSW", "IN-MH", "JP-13", "KR-11", "CN-BJ", "CN-NX", "BH")
                    .defaultProperties(AWSS3ProviderMetadata.defaultProperties());
        }

        public AWSS3SingleRegionProviderMetadata build() {
            return new AWSS3SingleRegionProviderMetadata(this);
        }

        public AWSS3SingleRegionProviderMetadata.Builder fromProviderMetadata(ProviderMetadata in) {
            super.fromProviderMetadata(in);
            return this;
        }
    }
}
