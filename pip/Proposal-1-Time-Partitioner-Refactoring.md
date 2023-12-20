# Proposal-1: Partitioner Refactoring

- *Author(s)*: Zike Yang
- *Proposal time*: 2023-12-20
- *Implemented*: NO
- *Released*: NO
- *Repository*: https://github.com/streamnative/pulsar-io-cloud-storage
- *Discussion Link*:

## TL;DR

The existing partitioner implementation lacks intuitiveness for user interaction, and its name does not accurately
reflect its functionality. This proposal aims to refactor the partitioner implementation to improve its intuitiveness
and ease of use. A new partitioner interface is introduced in this proposal, which includes two partitioners: Topic
Partitioner and Time Partitioner.

## Background knowledge

### Partitioner

The partitioner is utilized to determine the distribution or organization of messages when flushed to cloud storage.
The Cloud Storage sink connector offers two partitioners:

- **Simple partitioner**: This is the default partitioning method based on Pulsar partitions. In other words, data is
  partitioned according to the pre-existing partitions in Pulsar topics. For instance, a message for the
  topic `public/default/my-topic-partition-0` would be directed to the
  file `public/default/my-topic-partition-0/xxx.json`, where `xxx` signifies the message offset.

- **Time partitioner**: Data is partitioned according to the time it was flushed. Using the previous message as an
  example, if it was received on 2023-12-20, it would be directed
  to `public/default/my-topic-partition-0/2023-12-20/xxx.json`, where `xxx` also denotes the message offset.

## Motivation

- Incorrect implementation of time partitioner: The current implementation only adds time information to the file path
  , while both the Simple Partitioner and Time Partitioner partition messages based on the topic partition.
  The Time Partitioner is merely a special case of the Simple Partitioner. The existing time partitioner does not
  actually partition messages based on time.

- Inflexible current partitioner: It's hard for now to implement a correct Time Partitioner
  based on the current partitioner interface. The current connector first splits messages based on the topic, then
  allows the partitioner to
  generate the file path. As a result, all current partitioner implementations are based on the topic.

- Non-intuitive Partitioner interface:
  The [existing Partitioner interface](https://github.com/streamnative/pulsar-io-cloud-storage/blob/master/src/main/java/org/apache/pulsar/io/jcloud/partitioner/Partitioner.java)
  is not user-friendly. It has four methods, but it actually don't need so many methods. For instance, the expected
  behavior of `encodePartition` and `generatePartitionedPath` is overlapped.
  We should make its implementation simple and clear enough.

## Goals

### In Scope

- Refactor the existing partitioner implementation to improve its intuitiveness and ease of use.
- Introduce a new partitioner interface that includes two partitioners: Topic Partitioner and Time Partitioner.
- Ensure backward compatibility to avoid disrupting current partitioner usage.

### Out of Scope

- None

## High Level Design

A new `Partitioner` interface will be added, with two partitioner implementations: `TopicPartitioner`
and `TimePartitioner`.

The behavior of these partitioners is as follows:

- **Topic Partitioner**: Messages are partitioned according to the pre-existing partitions in the Pulsar topics. For
  instance, a message for the topic `public/default/my-topic-partition-0` would be directed to the
  file `public/default/my-topic-partition-0/xxx.json`, where `xxx` signifies the message offset.
- **Time Partitioner**: Messages are partitioned based on the timestamp at the time of flushing. For the aforementioned
  message, it would be directed to the file `1703037311.json`, where `1703037311` represents the flush timestamp of the
  first message in this file.

To ensure backward compatibility, the existing partitioner implementation will be maintained, and current user usage
will not be disrupted.

A Proof of Concept (PoC) implementation for this proposal can be found
at: https://github.com/streamnative/pulsar-io-cloud-storage/pull/845

## Detailed Design

### Design & Implementation Details

#### Partitioner Interface

The new partitioner interface would appear as follows:

```java
/**
 * The Partitioner interface offers a mechanism to categorize a list of records into distinct parts.
 */
public interface Partitioner {
    /**
     * The partition method takes a list of records and returns a map. Each key in the map represents a
     * unique partition, and the corresponding value is a list of records that belong to that partition.
     *
     * @param records A list of records to be partitioned. Each record is of the type GenericRecord.
     * @return A map where keys represent unique partitions and values are lists of records
     * associated with their respective partitions. The unique partition is consistently used as a file path in the cloud
     * storage system.
     */
    Map<String, List<Record<GenericRecord>>> partition(List<Record<GenericRecord>> records);
}
```

This new interface introduces a single method `partition`, simplifying and clarifying the interface.

#### Implementation of Partitioners

`Topic Partitioner` Implementation: This mirrors the default behavior of the current Simple Partitioner implementation.
It separates messages into multiple files based on the topic partition during the flush.

`Time Partitioner` Implementation: It segregates messages into multiple files based on the current timestamp during the
flush. The timestamp is chosen based on the first message in the file.

To implement these partitioners, we need to refactor the current connector implementation. This will allow the
partitioner to split records and generate the destination file path.

#### Backward Compatibility

A new `partitioner` configuration will be introduced to the Cloud Storage sink connector. This will not conflict with
the existing `partitionerType` configuration. The current `partitionerType` configuration should also be deprecated.

To ensure backward compatibility, a `Legacy Partitioner` will be introduced. If users wish to revert to previous
behavior, they can set the `partitioner` configuration to `legacy` to utilize the `Legacy Partitioner`. In this case,
the `partitionerType` is respected, allowing users to revert to legacy behavior.

This design's advantage is that it maintains the current partitioner implementation and makes it easier to remove the
legacy partitioner implementation and `partitionType` configuration in the future.

#### Configuration

Add a new configuration `partitioner` to the Cloud Storage sink connector. The value of this configuration can
be `topic`, `time` or `legacy`. The default value is `legacy`.

When it's set to the `legacy`, the `partitionerType` is respected. The user can fallback to the legacy behavior.

When it's set to the `topic` or `time`, the connector will use the new implementation of the partitioners.
The `partitionerType` configuration will be ignored.

## Security Considerations

This will not invoke any security issues.

## Backward & Forward Compatibility

### Revert

Set the `partitioner` configuration to `legacy` to fallback to the legacy behavior.

### Upgrade

Set the `partitioner` configuration to `topic` or `time` to use the new partitioner implementation.

## How will this be made available?

### Fully-managed product: Hosted / BYOC Cloud

This will be released in the next feature version of Cloud Storage sink connector.
The connector catalog will be updated and it will be available to use.

### Self-managed product: Platform / Private Cloud

This will be released in the next feature version of Cloud Storage sink connector.
And it will be available to use.

## Alternatives

None

## General Notes

The PR for the PoC implementation: https://github.com/streamnative/pulsar-io-cloud-storage/pull/845
