---
description: Google Cloud Storage sink Connector integrates Apache Pulsar with Google Cloud Storage.
features: ["Google Cloud Storage Connector integrates Apache Pulsar with Google Cloud Storage."]
alias: Google Cloud Storage Sink Connector
dockerfile: "https://hub.docker.com/r/streamnative/pulsar-io-cloud-storage"
---

The [Google Cloud Storage](https://cloud.google.com/storage/docs) sink connector pulls data from Pulsar topics and persists data to Google Cloud Storage buckets.

![](/docs/google-cloud-storage-sink.png)

## Quick start

### Prerequisites

The prerequisites for connecting an Google Cloud Storage sink connector to external systems include:

1. Create Cloud Storage buckets in Google Cloud.
2. Create the [Google cloud ServiceAccount](https://cloud.google.com/iam/docs/service-accounts-create) and create a public key certificate.
3. Create the [Google cloud Role](https://cloud.google.com/iam/docs/creating-custom-roles), ensure the Google Cloud role have the following permissions:
```text
- storage.buckets.get
- storage.buckets.list
- storage.objects.create
```
4. Grant the `ServiceAccount` the above `Role`.


### 1. Create a connector

Depending on the environment, there are several ways to create an Google Cloud Storage sink connector:

- [Create Connector on StreamNative Cloud](https://docs.streamnative.io/docs/connector-create).
- [Create Connector with Function worker](https://pulsar.apache.org/docs/3.0.x/io-quickstart/).
  Using this way requires you to download a **NAR** package to create a built-in or non-built-in connector. You can download the version you need from [here](https://github.com/streamnative/pulsar-io-cloud-storage/releases).
- [Create Connector with Function mesh](https://functionmesh.io/docs/connectors/run-connector).
  Using this way requires you to set the docker image. You can choose the version you want to launch from [here](https://hub.docker.com/r/streamnative/pulsar-io-cloud-storage/tags)

No matter how you create an Google Cloud Storage sink connector, the minimum configuration contains the following parameters.

```yaml
   configs:
     provider: "google-cloud-storage"
     gcsServiceAccountKeyFileContent: {{Public key certificate you created above}}
     bucket: {{Your bucket name}}
     formatType: "json"
     partitionerType: "PARTITION"
```

> * The configuration structure varies depending on how you create the Google Cloud Storage sink connector.
>   For example, some are **JSON**, some are **YAML**, and some are **Kubernetes YAML**. You need to adapt the configs to the corresponding format.
>
> * If you want to configure more parameters, see [Configuration Properties](#configuration-properties) for reference.

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

### 3. Display data on Google Cloud Storage console

You can see the object at public/default/{{Your topic name}}-partition-0/xxxx.json on the Google Cloud Storage console. Download and open it, the content is:

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

Before using the Google Cloud Storage sink connector, you need to configure it. This table outlines the properties and the descriptions.

| Name                              | Type    | Required | Default      | Description                                                                                                                                                                                                                                                                                                                                                                           |
|-----------------------------------|---------|----------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `provider`                        | String  | True     | null         | The Cloud Storage type, google cloud storage only supports the `google-cloud-storage` provider.                                                                                                                                                                                                                                                                                       |
| `bucket`                          | String  | True     | null         | The Cloud Storage bucket.                                                                                                                                                                                                                                                                                                                                                             |
| `formatType`                      | String  | True     | null         | The data format type. Available options are JSON, Avro, Bytes, or Parquet. By default, it is set to JSON.                                                                                                                                                                                                                                                                             |
| `partitionerType`                 | String  | True     | null         | The partitioning type. It can be configured by topic partitions or by time. By default, the partition type is configured by topic partitions.                                                                                                                                                                                                                                         |
| `gcsServiceAccountKeyFileContent` | String  | False    | ""           | The contents of the JSON service key file. If empty, credentials are read from `gcsServiceAccountKeyFilePath` file.                                                                                                                                                                                                                                                                   |
| `gcsServiceAccountKeyFilePath`    | String  | False    | ""           | Path to the GCS credentials file. If empty, the credentials file will be read from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.                                                                                                                                                                                                                                         |
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

## Advanced features

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
 
 
### Dead-letter topics

To use a dead-letter topic, you need to set `skipFailedMessages` to `false`, and set `--max-redeliver-count` and `--dead-letter-topic` when submit the connector with the `pulsar-admin` CLI tool. For more info about dead-letter topics, see the [Pulsar documentation](https://pulsar.apache.org/docs/en/concepts-messaging/#dead-letter-topic).
If a message fails to be sent to the Cloud Storage and there is a dead-letter topic, the connector will send the message to the dead-letter topic.

### Sink flushing only after batchTimeMs elapses

There is a scenario where the sink is only flushing whenever the `batchTimeMs` has elapsed, even though there are many messages waiting to be processed.
The reason for this is that the sink will only acknowledge messages after they are flushed to cloud storage but the broker stops sending messages when it reaches a certain limit of unacknowledged messages.
If this limit is lower or close to `batchSize`, the sink never receives enough messages to trigger a flush based on the amount of messages.
In this case please ensure the `maxUnackedMessagesPerConsumer` set in the broker configuration is sufficiently larger than the `batchSize` setting of the sink.