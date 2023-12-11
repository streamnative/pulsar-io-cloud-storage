---
description: Azure Blob Storage sink Connector integrates Apache Pulsar with Azure Blob Storage.
features: ["Azure Blob Storage Connector integrates Apache Pulsar with Azure Blob Storage."]
alias: Azure Blob Storage Sink Connector
dockerfile: "https://hub.docker.com/r/streamnative/pulsar-io-cloud-storage"
---

The [Azure Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-overview) sink connector pulls data from Pulsar topics and persists data to Azure Blob Storage containers.

![](/docs/azure-blob-storage-sink.png)

## Quick start

### Prerequisites

The prerequisites for connecting an Azure Blob Storage sink connector to external systems include:

1. Create Blob Storage container in Azure Cloud.
2. Get Storage account `Connection string`.


### 1. Create a connector

The following command shows how to use [pulsarctl](https://github.com/streamnative/pulsarctl) to create a `builtin` connector. If you want to create a `non-builtin` connector,
you need to replace `--sink-type cloud-storage-azure-blob` with `--archive /path/to/pulsar-io-cloud-storage.nar`. You can find the button to download the `nar` package at the beginning of the document.

{% callout title="For StreamNative Cloud User" type="note" %}
If you are a StreamNative Cloud user, you need [set up your environment](https://docs.streamnative.io/docs/connector-setup) first.
{% /callout %}

```bash
pulsarctl sinks create \
  --sink-type cloud-storage-azure-blob \
  --name azure-blob-sink \
  --tenant public \
  --namespace default \
  --inputs "Your topic name" \
  --parallelism 1 \
  --sink-config \
  '{
    "azureStorageAccountConnectionString": "Your azure blob storage account connection string",
    "provider": "azure-blob-storage",
    "bucket": "{Your container name",
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

### 3. Display data on Azure Blob Storage console

You can see the object at public/default/{{Your topic name}}-partition-0/xxxx.json on the Azure Blob Storage console. Download and open it, the content is:

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

Before using the Azure Blob Storage sink connector, you need to configure it. This table outlines the properties and the descriptions.

| Name                                  | Type    | Required | Default      | Description                                                                                                                                                                                                                                                                                                                                                                           |
|---------------------------------------|---------|----------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `provider`                            | String  | True     | null         | The Cloud Storage type, Azure Blob Storage only supports the `azure-blob-storage` provider.                                                                                                                                                                                                                                                                                           |
| `bucket`                              | String  | True     | null         | The Azure Blob Storage container name.                                                                                                                                                                                                                                                                                                                                                |
| `formatType`                          | String  | True     | "json"       | The data format type. Available options are `json`, `avro`, `bytes`, or `parquet`. By default, it is set to `json`.                                                                                                                                                                                                                                                                   |
| `partitionerType`                     | String  | True     | null         | The partitioning type. It can be configured by topic partitions or by time. By default, the partition type is configured by topic partitions.                                                                                                                                                                                                                                         |
| `azureStorageAccountConnectionString` | String  | False    | ""           | The Azure Blob Storage connection string. Required when authenticating via connection string.                                                                                                                                                                                                                                                                                         |
| `azureStorageAccountSASToken`         | String  | False    | ""           | The Azure Blob Storage account SAS token. Required when authenticating via SAS token.                                                                                                                                                                                                                                                                                                 |
| `azureStorageAccountName`             | String  | False    | ""           | The Azure Blob Storage account name. Required when authenticating via account name and account key.                                                                                                                                                                                                                                                                                   |
| `azureStorageAccountKey`              | String  | False    | ""           | The Azure Blob Storage account key. Required when authenticating via account name and account key.                                                                                                                                                                                                                                                                                    |
| `endpoint`                            | String  | False    | null         | The Azure Blob Storage endpoint. Required when authenticating via account name or SAS token.                                                                                                                                                                                                                                                                                          |
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
| `avroCodec`                           | String  | False    | snappy       | Compression codec used when formatType=`avro`. Available compression types are: none (no compression), deflate, bzip2, xz, zstandard, snappy.                                                                                                                                                                                                                                         |
| `parquetCodec`                        | String  | False    | gzip         | Compression codec used when formatType=`parquet`. Available compression types are: none (no compression), snappy, gzip, lzo, brotli, lz4, zstd.                                                                                                                                                                                                                                       |
| `jsonAllowNaN`                        | Boolean | False    | false        | Recognize 'NaN', 'INF', '-INF' as legal floating number values when formatType=`json`. Since JSON specification does not allow such values this is a non-standard feature and disabled by default.                                                                                                                                                                                    |

There are three methods to authenticate with Azure Blob Storage:
1. `azureStorageAccountConnectionString`: This method involves using the Azure Blob Storage connection string for authentication. It's the simplest method as it only requires the connection string.
2. `azureStorageAccountSASToken`: This method uses a Shared Access Signature (SAS) token for the Azure Blob Storage account. When using this method, you must also set the `endpoint`.
3. `azureStorageAccountName` and `azureStorageAccountKey`: This method uses the Azure Blob Storage account name and account key for authentication. Similar to the SAS token method, you must also set the `endpoint` when using this method.


## Advanced features

### Data format types

Azure Blob Storage Sink Connector provides multiple output format options, including JSON, Avro, Bytes, or Parquet. The default format is JSON.
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
If a message fails to be sent to the Azure Blob Storage and there is a dead-letter topic, the connector will send the message to the dead-letter topic.

### Sink flushing only after batchTimeMs elapses

There is a scenario where the sink is only flushing whenever the `batchTimeMs` has elapsed, even though there are many messages waiting to be processed.
The reason for this is that the sink will only acknowledge messages after they are flushed to the Azure Blob Storage but the broker stops sending messages when it reaches a certain limit of unacknowledged messages.
If this limit is lower or close to `batchSize`, the sink never receives enough messages to trigger a flush based on the amount of messages.
In this case please ensure the `maxUnackedMessagesPerConsumer` set in the broker configuration is sufficiently larger than the `batchSize` setting of the sink.
