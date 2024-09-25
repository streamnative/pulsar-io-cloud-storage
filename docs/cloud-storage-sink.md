---
dockerfile: "https://hub.docker.com/r/streamnative/pulsar-io-cloud-storage"
alias: Cloud Storage Sink Connector
---

The Cloud Storage sink connector supports exporting data from Pulsar topics to cloud storage (such as AWS S3 and Google GCS) either in Avro, JSON, Parquet or other formats. According to your environment, the Cloud Storage sink connector can guarantee exactly-once support for exporting data to cloud storage.

![](/docs/cloud-storage-sink.png)

# How to get

You can get the Cloud Storage sink connector using one of the following methods:

## Use it with Function Worker

- Download the NAR package from [here](https://github.com/streamnative/pulsar-io-cloud-storage/releases/download/v3.0.6.7/pulsar-io-cloud-storage-3.0.6.7.nar).

- Build it from the source code.

    1. Clone the source code to your machine.

       ```bash
       git clone https://github.com/streamnative/pulsar-io-cloud-storage.git
       ```

    2. Assume that `PULSAR_IO_CLOUD_STORAGE_HOME` is the home directory for the `pulsar-io-cloud-storage` repo. Build the connector in the `${PULSAR_IO_CLOUD_STORAGE_HOME}` directory.

       ```bash
       mvn clean install -DskipTests
       ```

       After the connector is successfully built, a `NAR` package is generated under the `target` directory.

       ```bash
       ls target
       pulsar-io-cloud-storage-3.0.6.7.nar
       ```

## Use it with Function Mesh

Pull the Cloud Storage connector Docker image from [here](https://hub.docker.com/r/streamnative/pulsar-io-cloud-storage).

# How to configure

Before using the Cloud Storage sink connector, you need to configure it.

You can create a configuration file (JSON or YAML) to set the following properties.

### Storage provider: AWS S3

| Name                            | Type    | Required | Default      | Description                                                                                                                                                                                                                                                                                                                                                                           |
|---------------------------------|---------|----------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `provider`                      | String  | True     | null         | The Cloud Storage type, such as `aws-s3`,`s3v2`(`s3v2` uses the AWS client but not the JCloud client).                                                                                                                                                                                                                                                                                |
| `accessKeyId`                   | String  | True     | null         | The Cloud Storage access key ID. It requires permission to write objects.                                                                                                                                                                                                                                                                                                             |
| `secretAccessKey`               | String  | True     | null         | The Cloud Storage secret access key.                                                                                                                                                                                                                                                                                                                                                  |
| `role`                          | String  | False    | null         | The Cloud Storage role.                                                                                                                                                                                                                                                                                                                                                               |
| `roleSessionName`               | String  | False    | null         | The Cloud Storage role session name.                                                                                                                                                                                                                                                                                                                                                  |
| `endpoint`                      | String  | True     | null         | The Cloud Storage endpoint.                                                                                                                                                                                                                                                                                                                                                           |
| `bucket`                        | String  | True     | null         | The Cloud Storage bucket.                                                                                                                                                                                                                                                                                                                                                             |
| `formatType`                    | String  | False    | "json"       | The data format type. Available options are JSON, Avro, Bytes, or Parquet. By default, it is set to JSON.                                                                                                                                                                                                                                                                             |
| `partitionerType`               | String  | False    | "partition"  | The partitioning type. It can be configured by topic partitions or by time. By default, the partition type is configured by topic partitions.                                                                                                                                                                                                                                         |
| `timePartitionPattern`          | String  | False    | "yyyy-MM-dd" | The format pattern of the time-based partitioning. For details, refer to the Java date and time format.                                                                                                                                                                                                                                                                               |
| `timePartitionDuration`         | String  | False    | "86400000"   | The time interval for time-based partitioning. Support formatted interval string, such as `30d`, `24h`, `30m`, `10s`, and also support number in milliseconds precision, such as `86400000` refers to `24h` or `1d`.                                                                                                                                                                  |
| `partitionerUseIndexAsOffset`   | Boolean | False    | false        | Whether to use the Pulsar's message index as offset or the record sequence. It's recommended if the incoming messages may be batched. The brokers may or not expose the index metadata and, if it's not present on the record, the sequence will be used. See [PIP-70](https://github.com/apache/pulsar/wiki/PIP-70%3A-Introduce-lightweight-broker-entry-metadata) for more details. |
| `batchSize`                     | int     | False    | 10           | The number of records submitted in batch.                                                                                                                                                                                                                                                                                                                                             |
| `batchTimeMs`                   | long    | False    | 1000         | The interval for batch submission.                                                                                                                                                                                                                                                                                                                                                    |
| `maxBatchBytes`                 | long    | False    | 10000000     | The maximum number of bytes in a batch.                                                                                                                                                                                                                                                                                                                                               |
| `sliceTopicPartitionPath`       | Boolean | False    | false        | When it is set to `true`, split the partitioned topic name into separate folders in the bucket path.                                                                                                                                                                                                                                                                                  |
| `withMetadata`                  | Boolean | False    | false        | Save message attributes to metadata.                                                                                                                                                                                                                                                                                                                                                  |
| `useHumanReadableMessageId`     | Boolean | False    | false        | Use a human-readable format string for messageId in message metadata. The messageId is in a format like `ledgerId:entryId:partitionIndex:batchIndex`. Otherwise, the messageId is a Hex-encoded string.                                                                                                                                                                               |
| `withTopicPartitionNumber`      | Boolean | False    | true         | When it is set to `true`, include the topic partition number to the object path.                                                                                                                                                                                                                                                                                                      |
| `bytesFormatTypeSeparator`      | String  | False    | "0x10"       | It is inserted between records for the `formatType` of bytes. By default, it is set to '0x10'. An input record that contains the line separator looks like multiple records in the output object.                                                                                                                                                                                     |
| `pendingQueueSize`              | int     | False    | 10           | The number of records buffered in queue. By default, it is equal to `batchSize`. You can set it manually.                                                                                                                                                                                                                                                                             |
| `useHumanReadableSchemaVersion` | Boolean | False    | false        | Use a human-readable format string for the schema version in the message metadata. If it is set to `true`, the schema version is in plain string format. Otherwise, the schema version is in hex-encoded string format.                                                                                                                                                               |
| `skipFailedMessages`            | Boolean | False    | false        | Configure whether to skip a message which it fails to be processed. If it is set to `true`, the connector will skip the failed messages by `ack` it. Otherwise, the connector will `fail` the message.                                                                                                                                                                                |
| `pathPrefix`                    | String  | False    | false        | If it is set, the output files are stored in a folder under the given bucket path. The `pathPrefix` must be in the format of `xx/xxx/`.                                                                                                                                                                                                                                               |
| `avroCodec`                     | String  | False    | snappy       | Compression codec used when formatType=`avro`. Available compression types are: null (no compression), deflate, bzip2, xz, zstandard, snappy.                                                                                                                                                                                                                                         |
| `parquetCodec`                  | String  | False    | gzip         | Compression codec used when formatType=`parquet`. Available compression types are: null (no compression), snappy, gzip, lzo, brotli, lz4, zstd.                                                                                                                                                                                                                                       |
| `jsonAllowNaN`                  | Boolean | False    | false        | Recognize 'NaN', 'INF', '-INF' as legal floating number values when formatType=`json`. Since JSON specification does not allow such values this is a non-standard feature and disabled by default.                                                                                                                                                                                    |

The provided AWS credentials must have permissions to access AWS resources. 
To use the Cloud Storage sink connector, the suggested permission policies for AWS S3 are:
- `s3:AbortMultipartUpload`
- `s3:GetObject*`
- `s3:PutObject*`
- `s3:List*`

If you do not want to provide `region` in the configuration, you should enable the `s3:GetBucketLocation` permission policy as well.

### Storage provider: Google Cloud Storage

| Name                              | Type    | Required | Default      | Description                                                                                                                                                                                                                                                                                                                                                                           |
|-----------------------------------|---------|----------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `provider`                        | String  | True     | null         | The Cloud Storage type, google cloud storage only supports the `google-cloud-storage` provider.                                                                                                                                                                                                                                                                                       |
| `gcsServiceAccountKeyFilePath`    | String  | False    | ""           | Path to the GCS credentials file. If empty, the credentials file will be read from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.                                                                                                                                                                                                                                         |
| `gcsServiceAccountKeyFileContent` | String  | False    | ""           | The contents of the JSON service key file. If empty, credentials are read from `gcsServiceAccountKeyFilePath` file.                                                                                                                                                                                                                                                                   |
| `bucket`                          | String  | True     | null         | The Cloud Storage bucket.                                                                                                                                                                                                                                                                                                                                                             |
| `formatType`                      | String  | False    | "json"       | The data format type. Available options are JSON, Avro, Bytes, or Parquet. By default, it is set to JSON.                                                                                                                                                                                                                                                                             |
| `partitionerType`                 | String  | False    | "partition"  | The partitioning type. It can be configured by topic partitions or by time. By default, the partition type is configured by topic partitions.                                                                                                                                                                                                                                         |
| `timePartitionPattern`            | String  | False    | "yyyy-MM-dd" | The format pattern of the time-based partitioning. For details, refer to the Java date and time format.                                                                                                                                                                                                                                                                               |
| `timePartitionDuration`           | String  | False    | "86400000"   | The time interval for time-based partitioning. Support formatted interval string, such as `30d`, `24h`, `30m`, `10s`, and also support number in milliseconds precision, such as `86400000` refers to `24h` or `1d`.                                                                                                                                                                  |
| `partitionerUseIndexAsOffset`     | Boolean | False    | false        | Whether to use the Pulsar's message index as offset or the record sequence. It's recommended if the incoming messages may be batched. The brokers may or not expose the index metadata and, if it's not present on the record, the sequence will be used. See [PIP-70](https://github.com/apache/pulsar/wiki/PIP-70%3A-Introduce-lightweight-broker-entry-metadata) for more details. |
| `batchSize`                       | int     | False    | 10           | The number of records submitted in batch.                                                                                                                                                                                                                                                                                                                                             |
| `batchTimeMs`                     | long    | False    | 1000         | The interval for batch submission.                                                                                                                                                                                                                                                                                                                                                    |
| `maxBatchBytes`                   | long    | False    | 10000000     | The maximum number of bytes in a batch.                                                                                                                                                                                                                                                                                                                                               |
| `sliceTopicPartitionPath`         | Boolean | False    | false        | When it is set to `true`, split the partitioned topic name into separate folders in the bucket path.                                                                                                                                                                                                                                                                                  |
| `withMetadata`                    | Boolean | False    | false        | Save message attributes to metadata.                                                                                                                                                                                                                                                                                                                                                  |
| `useHumanReadableMessageId`       | Boolean | False    | false        | Use a human-readable format string for messageId in message metadata. The messageId is in a format like `ledgerId:entryId:partitionIndex:batchIndex`. Otherwise, the messageId is a Hex-encoded string.                                                                                                                                                                               |
| `withTopicPartitionNumber`        | Boolean | False    | true         | When it is set to `true`, include the topic partition number to the object path.                                                                                                                                                                                                                                                                                                      |
| `bytesFormatTypeSeparator`        | String  | False    | "0x10"       | It is inserted between records for the `formatType` of bytes. By default, it is set to '0x10'. An input record that contains the line separator looks like multiple records in the output object.                                                                                                                                                                                     |
| `pendingQueueSize`                | int     | False    | 10           | The number of records buffered in queue. By default, it is equal to `batchSize`. You can set it manually.                                                                                                                                                                                                                                                                             |
| `useHumanReadableSchemaVersion`   | Boolean | False    | false        | Use a human-readable format string for the schema version in the message metadata. If it is set to `true`, the schema version is in plain string format. Otherwise, the schema version is in hex-encoded string format.                                                                                                                                                               |
| `skipFailedMessages`              | Boolean | False    | false        | Configure whether to skip a message which it fails to be processed. If it is set to `true`, the connector will skip the failed messages by `ack` it. Otherwise, the connector will `fail` the message.                                                                                                                                                                                |
| `pathPrefix`                      | String  | False    | false        | If it is set, the output files are stored in a folder under the given bucket path. The `pathPrefix` must be in the format of `xx/xxx/`.                                                                                                                                                                                                                                               |
| `avroCodec`                       | String  | False    | snappy       | Compression codec used when formatType=`avro`. Available compression types are: null (no compression), deflate, bzip2, xz, zstandard, snappy.                                                                                                                                                                                                                                         |
| `parquetCodec`                    | String  | False    | gzip         | Compression codec used when formatType=`parquet`. Available compression types are: null (no compression), snappy, gzip, lzo, brotli, lz4, zstd.                                                                                                                                                                                                                                       |
| `jsonAllowNaN`                    | Boolean | False    | false        | Recognize 'NaN', 'INF', '-INF' as legal floating number values when formatType=`json`. Since JSON specification does not allow such values this is a non-standard feature and disabled by default.                                                                                                                                                                                    |

#### Storage Provider: Azure Blob Storage

| Name                                  | Type    | Required | Default      | Description                                                                                                                                                                                                                                                                                                                                                                           |
|---------------------------------------|---------|----------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `provider`                            | String  | True     | null         | The Cloud Storage type. Azure Blob Storage only supports the `azure-blob-storage` provider.                                                                                                                                                                                                                                                                                           |
| `azureStorageAccountSASToken`         | String  | True     | ""           | The Azure Blob Storage account SAS token. Required when authenticating via SAS token.                                                                                                                                                                                                                                                                                                 |
| `azureStorageAccountName`             | String  | True     | ""           | The Azure Blob Storage account name. Required when authenticating via account name and account key.                                                                                                                                                                                                                                                                                   |
| `azureStorageAccountKey`              | String  | True     | ""           | The Azure Blob Storage account key. Required when authenticating via account name and account key.                                                                                                                                                                                                                                                                                    |
| `azureStorageAccountConnectionString` | String  | True     | ""           | The Azure Blob Storage connection string. Required when authenticating via connection string.                                                                                                                                                                                                                                                                                         |
| `endpoint`                            | String  | True     | null         | The Azure Blob Storage endpoint.                                                                                                                                                                                                                                                                                                                                                      |
| `bucket`                              | String  | True     | null         | The Cloud Storage bucket.                                                                                                                                                                                                                                                                                                                                                             |
| `formatType`                          | String  | False    | "json"       | The data format type. Available options are JSON, Avro, Bytes, or Parquet. By default, it is set to JSON.                                                                                                                                                                                                                                                                             |
| `partitionerType`                     | String  | False    | "partition"  | The partitioning type. It can be configured by topic partitions or by time. By default, the partition type is configured by topic partitions.                                                                                                                                                                                                                                         |
| `timePartitionPattern`                | String  | False    | "yyyy-MM-dd" | The format pattern of the time-based partitioning. For details, refer to the Java date and time format.                                                                                                                                                                                                                                                                               |
| `timePartitionDuration`               | String  | False    | "86400000"   | The time interval for time-based partitioning. Support formatted interval string, such as `30d`, `24h`, `30m`, `10s`, and also support number in milliseconds precision, such as `86400000` refers to `24h` or `1d`.                                                                                                                                                                  |
| `partitionerUseIndexAsOffset`         | Boolean | False    | false        | Whether to use the Pulsar's message index as offset or the record sequence. It's recommended if the incoming messages may be batched. The brokers may or not expose the index metadata and, if it's not present on the record, the sequence will be used. See [PIP-70](https://github.com/apache/pulsar/wiki/PIP-70%3A-Introduce-lightweight-broker-entry-metadata) for more details. |
| `batchSize`                           | int     | False    | 10           | The number of records submitted in batch.                                                                                                                                                                                                                                                                                                                                             |
| `batchTimeMs`                         | long    | False    | 1000         | The interval for batch submission.                                                                                                                                                                                                                                                                                                                                                    |
| `maxBatchBytes`                       | long    | False    | 10000000     | The maximum number of bytes in a batch.                                                                                                                                                                                                                                                                                                                                               |
| `sliceTopicPartitionPath`             | Boolean | False    | false        | When it is set to `true`, split the partitioned topic name into separate folders in the bucket path.                                                                                                                                                                                                                                                                                  |
| `withMetadata`                        | Boolean | False    | false        | Save message attributes to metadata.                                                                                                                                                                                                                                                                                                                                                  |
| `useHumanReadableMessageId`           | Boolean | False    | false        | Use a human-readable format string for messageId in message metadata. The messageId is in a format like `ledgerId:entryId:partitionIndex:batchIndex`. Otherwise, the messageId is a Hex-encoded string.                                                                                                                                                                               |
| `withTopicPartitionNumber`            | Boolean | False    | true         | When it is set to `true`, include the topic partition number to the object path.                                                                                                                                                                                                                                                                                                      |
| `bytesFormatTypeSeparator`            | String  | False    | "0x10"       | It is inserted between records for the `formatType` of bytes. By default, it is set to '0x10'. An input record that contains the line separator looks like multiple records in the output object.                                                                                                                                                                                     |
| `pendingQueueSize`                    | int     | False    | 10           | The number of records buffered in queue. By default, it is equal to `batchSize`. You can set it manually.                                                                                                                                                                                                                                                                             |
| `useHumanReadableSchemaVersion`       | Boolean | False    | false        | Use a human-readable format string for the schema version in the message metadata. If it is set to `true`, the schema version is in plain string format. Otherwise, the schema version is in hex-encoded string format.                                                                                                                                                               |
| `skipFailedMessages`                  | Boolean | False    | false        | Configure whether to skip a message which it fails to be processed. If it is set to `true`, the connector will skip the failed messages by `ack` it. Otherwise, the connector will `fail` the message.                                                                                                                                                                                |
| `pathPrefix`                          | String  | False    | false        | If it is set, the output files are stored in a folder under the given bucket path. The `pathPrefix` must be in the format of `xx/xxx/`.                                                                                                                                                                                                                                               |
| `avroCodec`                           | String  | False    | snappy       | Compression codec used when formatType=`avro`. Available compression types are: null (no compression), deflate, bzip2, xz, zstandard, snappy.                                                                                                                                                                                                                                         |
| `parquetCodec`                        | String  | False    | gzip         | Compression codec used when formatType=`parquet`. Available compression types are: null (no compression), snappy, gzip, lzo, brotli, lz4, zstd.                                                                                                                                                                                                                                       |
| `jsonAllowNaN`                        | Boolean | False    | false        | Recognize 'NaN', 'INF', '-INF' as legal floating number values when formatType=`json`. Since JSON specification does not allow such values this is a non-standard feature and disabled by default.                                                                                                                                                                                    |

### Data format types

Cloud Storage Sink Connector provides multiple output format options, including JSON, Avro, Bytes, or Parquet. The default format is JSON.
With current implementation, there are some limitations for different formats:

This table lists the Pulsar Schema types supported by the writers.

| Pulsar Schema  | Writer: Avro | Writer: JSON | Writer: Parquet | Writer: Bytes |
|----------------|--------------|--------------|-----------------|---------------|
| Primitive      | ✗            | ✔ *          | ✗               | ✔             |
| Avro           | ✔            | ✔            | ✔               | ✔             |
| Json           | ✔            | ✔            | ✔               | ✔             |
| Protobuf **    | ✔            | ✔            | ✔               | ✔             |
| ProtobufNative | ✔ ***        | ✗            | ✔               | ✔             |
> *: The JSON writer will try to convert the data with a `String` or `Bytes` schema to JSON-format data if convertable.
>
> **: The Protobuf schema is based on the Avro schema. It uses Avro as an intermediate format, so it may not provide the best effort conversion.
>
> ***: The ProtobufNative record holds the Protobuf descriptor and the message. When writing to Avro format, the connector uses [avro-protobuf](https://github.com/apache/avro/tree/master/lang/java/protobuf) to do the conversion.

This table lists the support of `withMetadata` configurations for different writer formats:

| Writer Format | `withMetadata` |
|---------------|----------------|
| Avro          | ✔              |
| JSON          | ✔              |
| Parquet       | ✔ *            |
| Bytes         | ✗              |

> *: When using `Parquet` with `PROTOBUF_NATIVE` format, the connector will write the messages with `DynamicMessage` format. When `withMetadata` is set to `true`, the connector will add `__message_metadata__` to the messages with `PulsarIOCSCProtobufMessageMetadata` format.
>
> For example, if a message `User` has the following schema:
> ```protobuf
> syntax = "proto3";
> message User {
>  string name = 1;
>  int32 age = 2;
> }
> ```
>
> When `withMetadata` is set to `true`, the connector will write the message `DynamicMessage` with the following schema:
> ```protobuf
> syntax = "proto3";
> message PulsarIOCSCProtobufMessageMetadata {
>  map<string, string> properties = 1;
>  string schema_version = 2;
>  string message_id = 3;
> }
> message User {
>  string name = 1;
>  int32 age = 2;
>  PulsarIOCSCProtobufMessageMetadata __message_metadata__ = 3;
> }
> ```
>

## Configure it with Function Worker

You can create a configuration file (JSON or YAML) to set the properties as below.

**Example**

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

## Configure it with Function Mesh

You can submit a [CustomResourceDefinitions (CRD)](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) to create a Cloud Storage sink connector. Using CRD makes Function Mesh naturally integrate with the Kubernetes ecosystem. For more information about Pulsar sink CRD configurations, see [here](https://functionmesh.io/docs/connectors/io-crd-config/sink-crd-config).

You can define a CRD file (YAML) to set the properties as below.

```yaml
apiVersion: compute.functionmesh.io/v1alpha1
kind: Sink
metadata:
  name: cloud-storage-sink-sample
spec:
  image: streamnative/pulsar-io-cloud-storage:3.0.6.7
  className: org.apache.pulsar.io.jcloud.sink.CloudStorageGenericRecordSink
  replicas: 1
  maxReplicas: 1
  input:
    topic: persistent://public/default/user-avro-topic
    typeClassName: “[B”
  sinkConfig:
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
  pulsar:
    pulsarConfig: "test-pulsar-sink-config"
  resources:
    limits:
      cpu: "0.2"
      memory: 1.1G
    requests:
      cpu: "0.1"
      memory: 1G
  java:
    jar: connectors/pulsar-io-cloud-storage-3.0.6.7.nar
  clusterName: test-pulsar
```

# How to use

You can use the Cloud Storage sink connector with Function Worker or Function Mesh.

## Use it with Function Worker

You can use the Cloud Storage sink connector as a non built-in connector or a built-in connector.

### Use it as non built-in connector

If you already have a Pulsar cluster, you can use the Cloud Storage sink connector as a non built-in connector directly.

This example shows how to create an Cloud Storage sink connector on a Pulsar cluster using the [`pulsar-admin sinks create`](https://pulsar.apache.org/tools/pulsar-admin/2.11.0-SNAPSHOT/#-em-create-em--30) command.

```
PULSAR_HOME/bin/pulsar-admin sinks create \
--archive pulsar-io-cloud-storage-3.0.6.7.nar \
--sink-config-file cloud-storage-sink-config.yaml \
--classname org.apache.pulsar.io.jcloud.sink.CloudStorageGenericRecordSink \
--name cloud-storage-sink
```

### Use it as built-in connector

You can make the Cloud Storage sink connector as a built-in connector and use it on a standalone cluster or on-premises cluster.

#### Standalone cluster

This example describes how to use the Cloud Storage sink connector to export data from Pulsar topics to cloud storage (such as AWS S3 and Google GCS) in standalone mode.

1. Prepare the AWS Cloud Storage service. In this example, we use `Cloud Storagemock` as an example.


    ```
    docker pull apachepulsar/s3mock:latest
    docker run -p 9090:9090 -e initialBuckets=pulsar-integtest apachepulsar/s3mock:latest
    ```

2. Put the `pulsar-io-cloud-storage-2.5.1.nar` in the Pulsar connector catalog.

    ```
    cp pulsar-io-cloud-storage-2.5.1.nar     $PULSAR_HOME/connectors/pulsar-io-cloud-storage-2.5.1.nar
    ```

3. Start Pulsar in the standalone mode.

    ```
    $PULSAR_HOME/bin/pulsar standalone
    ```

4. Run the Cloud Storage sink connector locally.

    ```
    $PULSAR_HOME/bin/pulsar-admin sink localrun --sink-config-file cloud-storage-sink-config.yaml
    ```

5. Send Pulsar messages. Currently, only Avro or JSON schema is supported.

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

#### On-premises cluster

This example explains how to create a Cloud Storage sink connector in an on-premises cluster.

1. Copy the NAR package of the Cloud Storage connector to the Pulsar connectors directory.

    ```
    cp pulsar-io-cloud-storage-3.0.6.7.nar $PULSAR_HOME/connectors/pulsar-io-cloud-storage-3.0.6.7.nar
    ```

2. Reload all [built-in connectors](https://pulsar.apache.org/docs/en/next/io-connectors/).

    ```
    PULSAR_HOME/bin/pulsar-admin sinks reload
    ```

3. Check whether the Cloud Storage sink connector is available on the list or not.

    ```
    PULSAR_HOME/bin/pulsar-admin sinks available-sinks
    ```

4. Create a Cloud Storage connector on a Pulsar cluster using the [`pulsar-admin sinks create`](https://pulsar.apache.org/tools/pulsar-admin/2.11.0-SNAPSHOT/#-em-create-em--30) command.

    ```
    PULSAR_HOME/bin/pulsar-admin sinks create \
    --sink-type cloud-storage \
    --sink-config-file cloud-storage-sink-config.yaml \
    --name cloud-storage-sink
    ```

## Use it with Function Mesh

This example demonstrates how to create Cloud Storage sink connector through Function Mesh.

### Prerequisites

- Create and connect to a [Kubernetes cluster](https://kubernetes.io/).

- Create a [Pulsar cluster](https://pulsar.apache.org/docs/en/kubernetes-helm/) in the Kubernetes cluster.

- [Install the Function Mesh Operator and CRD](https://functionmesh.io/docs/install-function-mesh/) into the Kubernetes cluster.

### Steps

1. Define the Cloud Storage sink connector with a YAML file and save it as `sink-sample.yaml`.

   This example shows how to publish the Cloud Storage sink connector to Function Mesh with a Docker image.

    ```yaml
    apiVersion: compute.functionmesh.io/v1alpha1
    kind: Sink
    metadata:
      name: cloud-storage-sink-sample
    spec:
      image: streamnative/pulsar-io-cloud-storage:3.0.6.7
      className: org.apache.pulsar.io.jcloud.sink.CloudStorageGenericRecordSink
      replicas: 1
      maxReplicas: 1
      input:
        topic: persistent://public/default/user-avro-topic
        typeClassName: “[B”
      sinkConfig:
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
      pulsar:
        pulsarConfig: "test-pulsar-sink-config"
      resources:
        limits:
          cpu: "0.2"
          memory: 1.1G
        requests:
          cpu: "0.1"
          memory: 1G
      java:
        jar: connectors/pulsar-io-cloud-storage-3.0.6.7.nar
      clusterName: test-pulsar
    ```

2. Apply the YAML file to create the Cloud Storage sink connector.

   **Input**

    ```
    kubectl apply -f <path-to-sink-sample.yaml>
    ```

   **Output**

    ```
    sink.compute.functionmesh.io/cloud-storage-sink-sample created
    ```

3. Check whether the Cloud Storage sink connector is created successfully.

   **Input**

    ```
    kubectl get all
    ```

   **Output**

    ```
    NAME                                READY   STATUS      RESTARTS   AGE
    pod/cloud-storage-sink-sample-0     1/1     Running     0          77s
    ```

   After that, you can produce and consume messages using the Cloud Storage sink connector between Pulsar and your cloud storage provider.
