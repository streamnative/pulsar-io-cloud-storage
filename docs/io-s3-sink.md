---
description: The S3 sink connector pulls messages from Pulsar topics and persist messages to S3.
author: ["ASF"]
contributors: ["ASF"]
language: Java
document: 
source: "https://github.com/streamnative/pulsar-io-S3/tree/v2.5.1/src/main/java/org/apache/pulsar/ecosystem/io/activemq"
license: Apache License 2.0
tags: ["Pulsar IO", "S3", "Sink"]
alias: S3 Sink
features: ["Use S3 sink connector to sync data from Pulsar"]
license_link: "https://www.apache.org/licenses/LICENSE-2.0"
icon: "http://activemq.apache.org/assets/img/activemq_logo_black_small.png"
download: "https://github.com/streamnative/pulsar-io-s3/releases/download/v2.5.1/pulsar-io-S3-2.5.1.nar"
support: StreamNative
support_link: https://streamnative.io
support_img: "/images/connectors/streamnative.png"
dockerfile: 
owner_name: ""
owner_img: ""
id: "io-S3-sink"
---

The S3 sink connector pulls messages from Pulsar topics and persist messages to S3.

# Installation

```
git clone https://github.com/streamnative/pulsar-io-s3.git
cd pulsar-io-s3/
mvn clean install -DskipTests
cp target/pulsar-io-s3-0.0.1.nar $PULSAR_HOME/pulsar-io-s3-0.0.1.nar
```

# Configuration 

The configuration of the S3 sink connector has the following properties.

## S3 sink connector configuration

| Name | Type|Required | Default | Description |
|------|----------|----------|---------|-------------|
| `accessKeyId` |String| true | " " (empty string) | The s3 accessKeyId. |
| `secretAccessKey` | String| true | " " (empty string) | Thes3 secretAccessKey. |
| `role` | String |false | " " (empty string) | Thes3 role. |
| `roleSessionName` | String|false | " " (empty string) | Thes3 role. |
| `bucket` | String|true | " " (empty string) | Thes3 bucket. |
| `endpoint` | String|false | " " (empty string) | Thes3 endpoint. |
| `formatType` | String|false | "json" | Save format type, json, avro, parquet, default json |
| `partitionerType` | String|false |"partition" | Partition type, by partition, by time, partition is used by default. |
| `timePartitionPattern` | String|false |"yyyy-MM-dd" | Format pattern by time partition, refer to java DateTimeFormat. |
| `timePartitionDuration` | String|false |"1d" | Time interval divided by time, such as 1d, 1h. |
| `batchSize` | int |false |10 | Number of records submitted in batch. |
| `batchTimeMs` | long |false |1000 | Interval for batch submission. |

## Configure S3 sink connector

Before using the S3 sink connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
       "tenant": "public",
       "namespace": "default",
       "name": "s3-sink",
       "inputs": [
          "user-json-topic",
          "user-avro-topic"
       ],
       "archive": "connectors/pulsar-io-s3-0.0.1.nar",
       "parallelism": 1,
       "configs": {
          "accessKeyId": "accessKeyId",
          "secretAccessKey": "secretAccessKey",
          "role": "none",
          "roleSessionName": "none",
          "bucket": "testBucket",
          "region": "local",
          "endpoint": "us-standard",
          "formatType": "parquet",
          "partitionerType": "time",
          "timePartitionPattern": "yyyy-MM-dd",
          "timePartitionDuration": "1d",
          "batchSize": 10,
          "batchTimeMs": 1000
       }
    }
    ```

* YAML

    ```yaml
    tenant: "public"
    namespace: "default"
    name: "s3-sink"
    inputs: 
      - "user-json-topic"
      - "user-avro-topic"
    archive: "connectors/pulsar-io-s3-0.0.1.nar"
    parallelism: 1
    
    configs:
      accessKeyId: "accessKeyId"
      secretAccessKey: "secretAccessKey"
      role: "none"
      roleSessionName: "none"
      bucket: "testBucket"
      region: "local"
      endpoint: "us-standard"
      formatType: "parquet"
      partitionerType: "time"
      timePartitionPattern: "yyyy-MM-dd"
      timePartitionDuration: "1d"
      batchSize: 10
      batchTimeMs: 1000
    ```

# Usage

1. Prepare S3 service.

    Please prepare the aws-s3 service you use.Test can use s3mock

    ```
    docker pull apachepulsar/s3mock:latest
    docker run -p 9090:9090 -e initialBuckets=pulsar-integtest apachepulsar/s3mock:latest
    ```

2. Put the `pulsar-io-s3-2.5.1.nar` in the pulsar connectors catalog.

    ```
    cp pulsar-io-s3-2.5.1.nar $PULSAR_HOME/connectors/pulsar-io-s3-2.5.1.nar
    ```

3. Start Pulsar in standalone mode.

    ```
    $PULSAR_HOME/bin/pulsar standalone
    ```

4. Run S3 sink locally.

    ```
    $PULSAR_HOME/bin/pulsar-admin sink localrun --sink-config-file s3-sink-config.yaml
    ```

5. Send Pulsar messages.

    *The topic schema can only use `avro` or `json`.*
    Java example:
    ```java
     try (
                PulsarClient pulsarClient = PulsarClient.builder()
                        .serviceUrl("pulsar://localhost:6650")
                        .build();
                Producer<TestRecord> producer = pulsarClient.newProducer(Schema.AVRO(TestRecord.class))
                        .topic("public/default/test-parquet-avro")
                        .create();
                ) {
                List<TestRecord> testRecords = Arrays.asList(
                        new TestRecord("key1", 1, null),
                        new TestRecord("key2", 1, new TestRecord.TestSubRecord("aaa"))
                );
                for (TestRecord record : testRecords) {
                    producer.send(record);
                }
            }
    ```

6. Valid S3 data.

    example: 
    use topic:  persistent://public/default/s3-topic

    The saved path consists of `basepath` and `partition`，
    
    - basepath: `public/default/s3-topic`
    - path by time Partition: `${basepath}/${timePartitionPattern}/${messageRecordSequenceId}.${formatType}`
        example: `public/default/s3-topic/2020-09-14/123456.parquet`
       
    - path by partition Partition: `${basepath}/partition-${partitionId}/${messageRecordSequenceId}.${formatType}`
        example: `public/default/s3-topic/partition-0/123456.parquet`