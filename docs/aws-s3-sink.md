---
description: AWS S3 sink Connector integrates Apache Pulsar with AWS S3.
features: ["AWS S3 Connector integrates Apache Pulsar with AWS S3."]
alias: AWS S3 Sink Connector
dockerfile: "https://hub.docker.com/r/streamnative/pulsar-io-cloud-storage"
---

The [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html) sink connector pulls data from Pulsar topics and persists data to AWS S3 buckets.

![](/docs/aws-s3-sink.png)

## Quick start

### Prerequisites

The prerequisites for connecting an AWS S3 sink connector to external systems include:

1. Create S3 buckets in AWS.
2. Create the [AWS User](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html) and create `AccessKey`(Please record `AccessKey` and `SecretAccessKey`).
3. Assign permissions to AWS User, and ensure they have the following permissions to the AWS S3. 
```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "VisualEditor0",
			"Effect": "Allow",
			"Action": [
				"s3:PutObject",
				"s3:AbortMultipartUpload"
			],
			"Resource": "{Your bucket arn}/*"
		}
	]
}
```

### 1. Create a connector

The following command shows how to use [pulsarctl](https://github.com/streamnative/pulsarctl) to create a `builtin` connector. If you want to create a `non-builtin` connector,
you need to replace `--sink-type cloud-storage-s3` with `--archive /path/to/pulsar-io-cloud-storage.nar`. You can find the button to download the `nar` package at the beginning of the document.

{% callout title="For StreamNative Cloud User" type="note" %}
If you are a StreamNative Cloud user, you need [set up your environment](https://docs.streamnative.io/docs/connector-setup) first.
{% /callout %}

```bash
pulsarctl sinks create \
  --sink-type cloud-storage-s3 \
  --name aws-s3-sink \
  --tenant public \
  --namespace default \
  --inputs "Your topic name" \
  --parallelism 1 \
  --sink-config \
  '{
    "accessKeyId": "Your AWS access key", 
    "secretAccessKey": "Your AWS secret access key",
    "provider": "s3v2",
    "bucket": "Your bucket name",
    "region": "Your AWS S3 region",
    "formatType": "json",
    "partitionerType": "PARTITION"
  }'
```

The `--sink-config` is the minimum necessary configuration for starting this connector, and it is a JSON string. You need to substitute the relevant parameters with your own.
If you want to configure more parameters, see [Configuration Properties](#configuration-properties) for reference.

{% callout title="Note" type="note" %}
You can also choose to use a variety of other tools to create a connector:
- [pulsar-admin](https://pulsar.apache.org/docs/3.1.x/io-use/): The command arguments for `pulsar-admin` are similar to those of `pulsarctl`. You can find an example for [StreamNative Cloud Doc](https://docs.streamnative.io/docs/connector-create#create-a-built-in-connector ).
- [RestAPI](https://pulsar.apache.org/sink-rest-api/?version=3.1.1): You can find an example for [StreamNative Cloud Doc](https://docs.streamnative.io/docs/connector-create#create-a-built-in-connector).
- [Terraform](https://github.com/hashicorp/terraform): You can find an example for [StreamNative Cloud Doc](https://docs.streamnative.io/docs/connector-create#create-a-built-in-connector).
- [Function Mesh](https://functionmesh.io/docs/connectors/run-connector): The docker image can be found at the beginning of the document.
{% /callout %}

### 2. Send messages to the topic

{% callout title="Note" type="note" %}
If your connector is created on StreamNative Cloud, you need to authenticate your clients. See [Build applications using Pulsar clients](https://docs.streamnative.io/docs/qs-connect#jumpstart-for-beginners) for more information.
{% /callout %}

``` java
    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
                             .serviceUrl("{{Your Pulsar URL}}")
                             .build();


        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("{{Your topic name}}")
                .create();

        for (int i = 0; i < 10; i++) {
            // JSON string containing a single character
            String message = "{\"test-message\": \"test-value\"}";
            producer.send(message);
        }

        producer.close();
        client.close();
    }
```

### 3. Display data on AWS S3 console

You can see the object at public/default/{{Your topic name}}-partition-0/xxxx.json on the AWS S3 console. Download and open it, the content is:

```text
{"test-message":"test-value"}
{"test-message":"test-value"}
{"test-message":"test-value"}
{"test-message":"test-value"}
{"test-message":"test-value"}
{"test-message":"test-value"}
{"test-message":"test-value"}
{"test-message":"test-value"}
{"test-message":"test-value"}
{"test-message":"test-value"}
```

## Configuration Properties

Before using the AWS S3 sink connector, you need to configure it. This table outlines the properties and the descriptions.

| Name                            | Type    | Required | Sensitive | Default      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|---------------------------------|---------|----------|-----------|--------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `provider`                      | String  | True     | false     | null         | The AWS S3 client type, such as `aws-s3`,`s3v2`(`s3v2` uses the AWS client but not the JCloud client).                                                                                                                                                                                                                                                                                                                                                                                             |
| `accessKeyId`                   | String  | True     | true      | null         | The AWS access key ID. It requires permission to write objects.                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `secretAccessKey`               | String  | True     | true      | null         | The AWS secret access key.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `bucket`                        | String  | True     | false     | null         | The AWS S3 bucket.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `formatType`                    | String  | True     | false     | "json"       | The data format type. Available options are `json`, `avro`, `bytes`, or `parquet`. By default, it is set to `json`.                                                                                                                                                                                                                                                                                                                                                                                |
| `partitionerType`               | String  | False    | false     | null         | The partitioning type. It can be configured by topic `PARTITION` or `TIME`. By default, the partition type is configured by topic partitions.                                                                                                                                                                                                                                                                                                                                                      |
| `region`                        | String  | False    | false     | null         | The AWS S3 region. Either the endpoint or region must be set.                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `endpoint`                      | String  | False    | false     | null         | The AWS S3 endpoint. Either the endpoint or region must be set.                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `role`                          | String  | False    | false     | null         | The AWS role.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `roleSessionName`               | String  | False    | false     | null         | The AWS role session name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `timePartitionPattern`          | String  | False    | false     | "yyyy-MM-dd" | The format pattern of the time-based partitioning. For details, refer to the Java date and time format.                                                                                                                                                                                                                                                                                                                                                                                            |
| `timePartitionDuration`         | String  | False    | false     | "86400000"   | The time interval for time-based partitioning. Support formatted interval string, such as `30d`, `24h`, `30m`, `10s`, and also support number in milliseconds precision, such as `86400000` refers to `24h` or `1d`.                                                                                                                                                                                                                                                                               |
| `pathPrefix`                    | String  | False    | false     | false        | If it is set, the output files are stored in a folder under the given bucket path. The `pathPrefix` must be in the format of `xx/xxx/`.                                                                                                                                                                                                                                                                                                                                                            |
| `partitionerWithTopicName`      | Boolean | False    | false     | true         | Indicates whether to include the topic name in the file path. Default is true. If not included, the path like: `pathPrefix/24.45.0.json`                                                                                                                                                                                                                                                                                                                                                           |
| `partitionerUseIndexAsOffset`   | Boolean | False    | false     | false        | Whether to use the Pulsar's message index as offset or the record sequence. It's recommended if the incoming messages may be batched. The brokers may or not expose the index metadata and, if it's not present on the record, the sequence will be used. See [PIP-70](https://github.com/apache/pulsar/wiki/PIP-70%3A-Introduce-lightweight-broker-entry-metadata) for more details.                                                                                                              |
| `withTopicPartitionNumber`      | Boolean | False    | false     | true         | When it is set to `true`, include the topic partition number to the object path.                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `sliceTopicPartitionPath`       | Boolean | False    | false     | false        | When it is set to `true`, split the partitioned topic name into separate folders in the bucket path.                                                                                                                                                                                                                                                                                                                                                                                               |
| `batchSize`                     | int     | False    | false     | 10           | The number of records submitted in batch.                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `batchTimeMs`                   | long    | False    | false     | 1000         | The interval for batch submission.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `maxBatchBytes`                 | long    | False    | false     | 10000000     | The maximum number of bytes in a batch.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `batchModel`                    | Enum    | False    | false     | BLEND        | Determines how records are batched. Options: `BLEND`, `PARTITIONED`. The BLEND which combines all topic records into a single batch, optimizing for throughput, and PARTITIONED which batches records separately for each topic, maintaining topic-level separation. Note: When set to PARTITIONED, the connector will cache data up to the size of the number of subscribed topics multiplied by maxBatchBytes. This means you need to anticipate the connector's memory requirements in advance. |
| `skipFailedMessages`            | Boolean | False    | false     | false        | Configure whether to skip a message which it fails to be processed. If it is set to `true`, the connector will skip the failed messages by `ack` it. Otherwise, the connector will `fail` the message.                                                                                                                                                                                                                                                                                             |
| `withMetadata`                  | Boolean | False    | false     | false        | Save message attributes to metadata.                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `useHumanReadableMessageId`     | Boolean | False    | false     | false        | Use a human-readable format string for messageId in message metadata. The messageId is in a format like `ledgerId:entryId:partitionIndex:batchIndex`. Otherwise, the messageId is a Hex-encoded string.                                                                                                                                                                                                                                                                                            |
| `useHumanReadableSchemaVersion` | Boolean | False    | false     | false        | Use a human-readable format string for the schema version in the message metadata. If it is set to `true`, the schema version is in plain string format. Otherwise, the schema version is in hex-encoded string format.                                                                                                                                                                                                                                                                            |
| `includeTopicToMetadata`        | Boolean | False    | false     | false        | Include the topic name to the metadata.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `includePublishTimeToMetadata`  | Boolean | False    | false     | false        | Include the message publish time to the metadata as a timestamp.                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `includeMessageKeyToMetadata`   | Boolean | False    | false     | false        | Include the message key to the metadata as a string.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `avroCodec`                     | String  | False    | false     | snappy       | Compression codec used when formatType=`avro`. Available compression types are: none (no compression), deflate, bzip2, xz, zstandard, snappy.                                                                                                                                                                                                                                                                                                                                                      |
| `parquetCodec`                  | String  | False    | false     | gzip         | Compression codec used when formatType=`parquet`. Available compression types are: none (no compression), snappy, gzip, lzo, brotli, lz4, zstd.                                                                                                                                                                                                                                                                                                                                                    |
| `jsonAllowNaN`                  | Boolean | False    | false     | false        | Recognize 'NaN', 'INF', '-INF' as legal floating number values when formatType=`json`. Since JSON specification does not allow such values this is a non-standard feature and disabled by default.                                                                                                                                                                                                                                                                                                 |
| `bytesFormatTypeSeparator`      | String  | False    | false     | "0x10"       | It is inserted between records for the `formatType` of bytes. By default, it is set to '0x10'. An input record that contains the line separator looks like multiple records in the output object.                                                                                                                                                                                                                                                                                                  |

## Advanced features

### Data format types

AWS S3 Sink Connector provides multiple output format options, including JSON, Avro, Bytes, or Parquet. The default format is JSON.
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
 
 
### Dead-letter topics

To use a dead-letter topic, you need to set `skipFailedMessages` to `false`, and set `--max-redeliver-count` and `--dead-letter-topic` when submit the connector with the `pulsar-admin` CLI tool. For more info about dead-letter topics, see the [Pulsar documentation](https://pulsar.apache.org/docs/en/concepts-messaging/#dead-letter-topic).
If a message fails to be sent to the AWS S3 and there is a dead-letter topic, the connector will send the message to the dead-letter topic.

### Sink flushing only after batchTimeMs elapses

There is a scenario where the sink is only flushing whenever the `batchTimeMs` has elapsed, even though there are many messages waiting to be processed.
The reason for this is that the sink will only acknowledge messages after they are flushed to AWS S3 but the broker stops sending messages when it reaches a certain limit of unacknowledged messages.
If this limit is lower or close to `batchSize`, the sink never receives enough messages to trigger a flush based on the amount of messages.
In this case please ensure the `maxUnackedMessagesPerConsumer` set in the broker configuration is sufficiently larger than the `batchSize` setting of the sink.

### Partitioner Type

There are two types of partitioner:

- **PARTITION**: This is the default partitioning method based on Pulsar partitions. In other words, data is
  partitioned according to the pre-existing partitions in Pulsar topics. For instance, a message for the
  topic `public/default/my-topic-partition-0` would be directed to the
  file `public/default/my-topic-partition-0/xxx.json`, where `xxx` signifies the earliest messageId(Format: `ledgerId.entryId.batchIndex`)/offset(Enable config: `partitionerUseIndexAsOffset`) in this file.

- **TIME**: Data is partitioned according to the time it was flushed. Using the previous message as an
  example, if it was received on 2023-12-20, it would be directed
  to `public/default/my-topic-partition-0/2023-12-20/xxx.json`, where `xxx` also denotes the earliest messageId(Format: `ledgerId.entryId.batchIndex`)/offset(Enable config: `partitionerUseIndexAsOffset`) in this file.
