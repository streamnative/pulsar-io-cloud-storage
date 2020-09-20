---
description: The S3 sink connector pulls messages from Pulsar topics and persists messages to S3.
author: ["ASF"]
contributors: ["ASF"]
language: Java
document: 
source: "https://github.com/streamnative/pulsar-io-S3/tree/v2.5.1/src/main/java/org/apache/pulsar/io/s3"
license: Apache License 2.0
tags: ["Pulsar IO", "S3", "Sink"]
alias: S3 Sink
features: ["Use the S3 sink connector to sync data from Pulsar"]
license_link: "https://www.apache.org/licenses/LICENSE-2.0"
icon: "https://a0.awsstatic.com/libra-css/images/logos/aws_logo_smile_179x109.png"
download: "https://github.com/streamnative/pulsar-io-s3/releases/download/v2.5.1/pulsar-io-S3-2.5.1.nar"
support: StreamNative
support_link: https://streamnative.io
support_img: "/images/connectors/streamnative.png"
dockerfile: 
owner_name: ""
owner_img: ""
id: "io-S3-sink"
---

The S3 sink connector pulls messages from Pulsar topics and persists messages to S3.

# Installation

```
git clone https://github.com/streamnative/pulsar-io-s3.git
cd pulsar-io-s3/
mvn clean install -DskipTests
cp target/pulsar-io-s3-0.0.1.nar $PULSAR_HOME/pulsar-io-s3-0.0.1.nar
```

# Configuration 

The S3 sink connector supports the following properties.

## S3 sink connector configuration

| Name | Type|Required | Default | Description |
|------|----------|----------|---------|-------------|
| `accessKeyId` |String| True | " " (empty string) | The S3 access Key ID. |
| `secretAccessKey` | String| True | " " (empty string) | The S3 secret access Key. |
| `role` | String |False | " " (empty string) | The S3 role. |
| `roleSessionName` | String| False | " " (empty string) | The S3 role. |
| `bucket` | String| True | " " (empty string) | The S3 bucket. |
| `endpoint` | String| False | " " (empty string) | The S3 endpoint. |
| `formatType` | String| False | "json" | The data format type: JSON, Avro, or Parquet. By default, it is set to JSON. |
| `partitionerType` | String| False |"partition" | The partition type. It can be configured by partition or time. By default, the partition type is configured by partition. |
| `timePartitionPattern` | String| False |"yyyy-MM-dd" | The format pattern of the time partition. For details, refer to the Java date and time format. |
| `timePartitionDuration` | String| False |"1d" | The time interval divided by time, such as 1d, or 1h. |
| `batchSize` | int | False |10 | The number of records submitted in batch. |
| `batchTimeMs` | long | False |1000 | The interval for batch submission. |

## Configure S3 sink connector

Before using the S3 sink connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
       "tenant": "public",
       "namespace": "default",
       "name": "s3-sink",
       "inputs": [
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

1. Prepare the AWS S3 service. In this example, we use `s3mock` as an example.


    ```
    docker pull apachepulsar/s3mock:latest
    docker run -p 9090:9090 -e initialBuckets=pulsar-integtest apachepulsar/s3mock:latest
    ```

2. Put the `pulsar-io-s3-2.5.1.nar` in the Pulsar connector catalog.

    ```
    cp pulsar-io-s3-2.5.1.nar $PULSAR_HOME/connectors/pulsar-io-s3-2.5.1.nar
    ```

3. Start Pulsar in the standalone mode.

    ```
    $PULSAR_HOME/bin/pulsar standalone
    ```

4. Run the S3 sink connector locally.

    ```
    $PULSAR_HOME/bin/pulsar-admin sink localrun --sink-config-file s3-sink-config.yaml
    ```

5. Send Pulsar messages. In this example, the schema of the topic only supports `avro` or `json`.

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

6. Validate S3 data.
    To get the path, you can use the [jclould](https://jclouds.apache.org/start/install/) to verify the file, as shown below.
    ```java
    Properties overrides = new Properties();
    overrides.put(“jclouds.s3.virtual-host-buckets”, “false”);
    BlobStoreContext blobStoreContext = ContextBuilder.newBuilder(“aws-s3”)
            .credentials(
                    “accessKeyId”,
                    “secretAccessKey”
            )
            .endpoint(“http://localhost:9090”) // replace to s3mock url
            .overrides(overrides)
            .buildView(BlobStoreContext.class);
    BlobStore blobStore = blobStoreContext.getBlobStore();
    final long sequenceId = FunctionCommon.getSequenceId(message.getMessageId());
    final String path = “public/default/test-parquet-avro” + File.separator + “2020-09-14" + File.separator + sequenceId + “.parquet”;
    final boolean blobExists = blobStore.blobExists(“testBucket”, path);
    Assert.assertTrue(“the sink record does not exist”, blobExists);
    ```
    You can find the sink data in your `testBucket` Bucket. The path is something like this `public/default/test-parquet-avro/2020-09-14/1234.parquet`.
    The path consists of three parts, the basic part of the topic, partition information, and format suffix.
    - Basic part of topic: `public/default/test-parquet-avro/`
        This part consists of the tenant, namespace, and topic name of the input topic.
    - Partition information: `2020-09-14/${messageSequenceId}`
        The date is generated based on the `partitionerType` parameter in the configuration. and the `${messageSequenceId}` is generated by `FunctionCommon.getSequenceId(message.getMessageId())`.
    - Format suffix: `.parquet`
        This part is generated based on the `formatType` parameter in the configuration.
