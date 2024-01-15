# Proposal-2: Support including the topic name to the metadata

- *Author(s)*: Zike Yang
- *Proposal time*: 2024-01-10
- *Implemented*: NO
- *Released*: NO
- *Repository*: https://github.com/streamnative/pulsar-io-cloud-storage
- *Discussion Link*:

## TL;DR

This proposal introduces a new configuration `includeTopicName` to the Cloud Storage sink connector. When
activated(`true`), the connector will include the topic name to the metadata in the sink file.

## Background knowledge

### Metadata data format

The connector will include the metadata to the sink file for each message. Take the JSON format as an example, the data
in the file would be like:

```json
{
  "key": "value",
  "__message_metadata__": {
    "schemaVersion": "AAAAAAAAAAA=",
    "messageId": "CN4HEAgYADAA",
    "properties": {}
  }
}
```

The metadata will be put into the key `__message_metadata__` with a map type.

## Motivation

Currently, we don't include the topic name to the metadata. And there is not intuitive workaround for it.

## Goals

### In Scope

- Support including the Pulsar topic name into the metadata.

### Out of Scope

- None

## High Level Design

Introduce a new configuration `includeTopicToMetadata` to support including the Pulsar topic name into the metadata.

The new data format of the cloud storage format would be like:

```json
{
  "key": "value",
  "__message_metadata__": {
    "messageId": "CAgQADAA",
    "topic": "persistent://public/default/test-s3",
    "properties": {}
  }
}
```

A new key, `topic`, would be added to the metadata, containing the Pulsar topic name.

## Detailed Design

### Design & Implementation Details

#### Configuration

Introduce a new boolean configuration `includeTopicToMetadata` to the Cloud Storage sink connector. The default value
is `false`.

Upon setting this to `true`, the Pulsar topic name will be appended to the metadata.

## Security Considerations

This will not invoke any security issues.

## Backward & Forward Compatibility

### Revert

Set `includeTopicToMetadata` to `false` to rollback to the previous behavior.

### Upgrade

Set `includeTopicToMetadata` to `true` to enable this new feature.

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

The implementation PR is: https://github.com/streamnative/pulsar-io-cloud-storage/pull/836
After this proposal is approved, we could merge the PR to the master branch and release the new version of the
connector.
