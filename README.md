# Cloud Storage Sink Connector for Pulsar

The Cloud Storage sink connector supports exporting data from Pulsar topics to cloud storage (such as AWS S3 and Google GCS) either in Avro, JSON, Parquet or other formats. According to your environment, the Cloud Storage sink connector can guarantee exactly-once support for exporting data to cloud storage.

## Installation

There are two ways to install the Cloud Storage sink connector.

- Install the the Cloud Storage sink connector through the [NAR file](https://github.com/streamnative/pulsar-io-cloud-storage/releases). 
- Install the the Cloud Storage sink connector from the source code.

To build the the Cloud Storage sink connector from the source code, follow these steps.

1. Clone the project from GitHub to your local.

    ```bash
    git clone https://github.com/streamnative/pulsar-io-cloud-storage.git
    cd pulsar-io-cloud-storage
    ```
2. Build the project.
    
    ```bash
    mvn clean install -DskipTests
    ```
    You can find the NAR file in the following directory.
    ```bash
    ./pulsar-io-cloud-storage/target/pulsar-io-cloud-storage-${version}.nar
    ```

## Configuration 

The Cloud Storage sink connector supports the following properties.

### Cloud Storage sink connector configuration

| Name | Type|Required | Default | Description |
|------|----------|----------|---------|-------------|
| `provider` |String| True | null | The Cloud Storage type, such as `aws-s3`,`google-cloud-storage`.|
| `accessKeyId` |String| True | null | The Cloud Storage access key ID. It requires permission to write objects. |
| `secretAccessKey` | String| True | null | The Cloud Storage secret access key. |
| `role` | String |False | null | The Cloud Storage role. |
| `roleSessionName` | String| False | null | The Cloud Storage role session name. |
| `bucket` | String| True | null | The Cloud Storage bucket. |
| `endpoint` | String| True | null | The Cloud Storage endpoint. |
| `formatType` | String| False | "json" | The data format type. Available options are JSON, Avro, Bytes, or Parquet. By default, it is set to JSON. |
| `partitionerType` | String| False |"partition" | The partitioning type. It can be configured by topic partitions or by time. By default, the partition type is configured by topic partitions. |
| `timePartitionPattern` | String| False |"yyyy-MM-dd" | The format pattern of the time-based partitioning. For details, refer to the Java date and time format. |
| `timePartitionDuration` | String| False |"86400000" | The time interval for time-based partitioning, support formatted interval string, such as `30d`, `24h`, `30m`, `10s`, also support number in milliseconds precision, such as `86400000` refers to `24h` or `1d`. |
| `batchSize` | int | False |10 | The number of records submitted in batch. |
| `batchTimeMs` | long | False |1000 | The interval for batch submission. |
| `sliceTopicPartitionPath` | Boolean| False |false | When it is set to `true`, split the partitioned topic name into separate folders in the bucket path. |
| `withMetadata` | Boolean | False |false | Save message attributes to metadata. |
| `useHumanReadableMessageId` | Boolean | False |false | Use human-readable format string for messageId in message metadata, the messageId will be in format like `ledgerId:entryId:partitionIndex:batchIndex`. Otherwise, the messageId will be in Hex-encoded string.|
| `withTopicPartitionNumber` | Boolean | False |true | When it is set to `true`, include topic partition number to the object path. |
| `bytesFormatTypeSeparator` | String | False |"0x10" | It is inserted between records for the `formatType` of bytes. By default, it is set to '0x10'. An input record that contains the line separator looks like multiple records in the output object. |
| `gcsServiceAccountKeyFilePath` | String | False | "" | Path to the GCS credentials file. If empty, the credentials file will be read from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable. |
| `gcsServiceAccountKeyFileContent` | String | False | "" | The contents of the JSON service key file. If empty, credentials are read from `gcsServiceAccountKeyFilePath` file. |
| `pendingQueueSize` | int | False | 20 | The number of records buffered in queue, by default it will always be `batchSize * 2`, and it can be set manually. |

### Configure Cloud Storage sink connector

Before using the Cloud Storage sink connector, you need to create a configuration file through one of the following methods.

* JSON 

    ```json
    {
       "tenant": "public",
       "namespace": "default",
       "name": "cloud-storage-sink",
       "inputs": [
          "user-avro-topic"
       ],
       "archive": "connectors/pulsar-io-cloud-storage-0.0.1.nar",
       "parallelism": 1,
       "configs": {
          "provider": "aws-s3",
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
    name: "Cloud Storage-sink"
    inputs: 
      - "user-avro-topic"
    archive: "connectors/pulsar-io-cloud-storage-0.0.1.nar"
    parallelism: 1
    
    configs:
      provider: "aws-s3",
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

## Usage

1. Prepare the AWS Cloud Storage service. In this example, we use `Cloud Storagemock` as an example.


    ```
    docker pull apachepulsar/s3mock:latest
    docker run -p 9090:9090 -e initialBuckets=pulsar-integtest apachepulsar/s3mock:latest
    ```

2. Put the `pulsar-io-cloud-storage-2.5.1.nar` in the Pulsar connector catalog.

    ```
    cp pulsar-io-cloud-storage-2.5.1.nar $PULSAR_HOME/connectors/pulsar-io-cloud-storage-2.5.1.nar
    ```

3. Start Pulsar in the standalone mode.

    ```
    $PULSAR_HOME/bin/pulsar standalone
    ```

4. Run the Cloud Storage sink connector locally.

    ```
    $PULSAR_HOME/bin/pulsar-admin sink localrun --sink-config-file cloud-storage-sink-config.yaml
    ```

5. Send Pulsar messages. Currently, Avro or JSON mode supports formatType json, avro, parquet. No schema mode can only use bytes formatType.

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

6. Validate Cloud Storage data.

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
    You can find the data in your `testBucket` bucket. The path is something like `public/default/test-parquet-avro/2020-09-14/1234.parquet`.
    The path consists of three parts, the basic part of the topic, partition information, and format suffix.
    
    - Basic part of topic: `public/default/test-parquet-avro/`
        This part consists of the name of the tenant, namespace, and the input topic.
    - Partition information: `2020-09-14/${messageSequenceId}`
        The date is generated based on the `partitionerType` parameter in the configuration. And the `${messageSequenceId}` is generated by `FunctionCommon.getSequenceId(message.getMessageId())`.
    - Format suffix: `.parquet`
        This part is generated based on the `formatType` parameter in the configuration.

## Permissions

### AWS S3 permission policies

The suggested permission policies for AWS S3 are:
- `s3:AbortMultipartUpload`
- `s3:GetObject*`
- `s3:PutObject*`
- `s3:List*`

If you do not want to provide `region` in the configuration, you should enable `s3:GetBucketLocation` permission policy as well. 
