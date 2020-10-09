## Pulsar IO :: Cloud Storage

Cloud Storage Sink connector for Pulsar

The Cloud Storage Sink connector supports exporting data from Pulsar Topic to cloud storage in Avro, json, parquet and other formats, such as aws-s3, google-GCS, etc. According to your environment, the Cloud Storage sink connector can guarantee exactly-once support for exporting to cloud storage.

# Installation

You can download Cloud Storage nar file from the [Cloud Storage releases](https://github.com/streamnative/pulsar-io-cloud-storage/releases).

To build from code, complete the following steps:

1. Clone the project from GitHub to your local.

```bash
git clone https://github.com/streamnative/pulsar-io-cloud-storage.git
cd pulsar-io-cloud-storage
```

2. Build the project.
```bash
mvn clean install -DskipTests
```
You can find the nar file in the following directory.
```bash
./pulsar-io-cloud-storage/target/pulsar-io-cloud-storage-${version}.nar
```

# Configuration 

The Cloud Storage sink connector supports the following properties.


## Cloud Storage sink connector configuration

| Name | Type|Required | Default | Description |
|------|----------|----------|---------|-------------|
| `provider` |String| True | " " (empty string) | The Cloud Storage type. for example, `aws-s3`,`gcs`|
| `accessKeyId` |String| True | " " (empty string) | The Cloud Storage access key ID. requires permission to write objects |
| `secretAccessKey` | String| True | " " (empty string) | The Cloud Storage secret access key. |
| `role` | String |False | " " (empty string) | The Cloud Storage role. |
| `roleSessionName` | String| False | " " (empty string) | The Cloud Storage role session name. |
| `bucket` | String| True | " " (empty string) | The Cloud Storage bucket. |
| `endpoint` | String| False | " " (empty string) | The Cloud Storage endpoint. |
| `formatType` | String| False | "json" | The data format type: JSON, Avro, or Parquet. By default, it is set to JSON. |
| `partitionerType` | String| False |"partition" | The partition type. It can be configured by partition or time. By default, the partition type is configured by partition. |
| `timePartitionPattern` | String| False |"yyyy-MM-dd" | The format pattern of the time partition. For details, refer to the Java date and time format. |
| `timePartitionDuration` | String| False |"1d" | The time interval divided by time, such as 1d, or 1h. |
| `batchSize` | int | False |10 | The number of records submitted in batch. |
| `batchTimeMs` | long | False |1000 | The interval for batch submission. |

## Configure Cloud Storage sink connector

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

# Usage

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
    You can find the sink data in your `testBucket` Bucket. The path is something like `public/default/test-parquet-avro/2020-09-14/1234.parquet`.
    The path consists of three parts, the basic part of the topic, partition information, and format suffix.
    
    - Basic part of topic: `public/default/test-parquet-avro/`
        This part consists of the tenant, namespace, and topic name of the input topic.
    - Partition information: `2020-09-14/${messageSequenceId}`
        The date is generated based on the `partitionerType` parameter in the configuration. And the `${messageSequenceId}` is generated by `FunctionCommon.getSequenceId(message.getMessageId())`.
    - Format suffix: `.parquet`
        This part is generated based on the `formatType` parameter in the configuration.






