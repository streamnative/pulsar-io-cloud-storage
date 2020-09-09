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
          "accessKeyId": "sSs7gUYFtwVIJh",
          "secretAccessKey": "8WGoYQnJV9DC9JaeA95Wj",
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
      accessKeyId: "sSs7gUYFtwVIJh"
      secretAccessKey: "8WGoYQnJV9DC9JaeA95Wj"
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

    Please prepare the aws-s3 service you use.Test can use mockaws

    ```
    docker pull rmohr/activemq
    docker run -p 61616:61616 -p 8161:8161 rmohr/activemq
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

    ```
    $PULSAR_HOME/bin/pulsar-client produce public/default/user-json-topic --messages '{"age":1,"status":"ok"}' -n 10
    ```

6. Check S3 data.

    Use the test method `receiveMessage` of the class `org.apache.pulsar.ecosystem.io.activemq.ActiveMQDemo` 
    to consume ActiveMQ messages.

    ```
    @Test
    private void receiveMessage() throws JMSException, InterruptedException {
    
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
    
        @Cleanup
        Connection connection = connectionFactory.createConnection();
        connection.start();
    
        @Cleanup
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    
        Destination destination = session.createQueue("user-op-queue-pulsar");
    
        @Cleanup
        MessageConsumer consumer = session.createConsumer(destination);
    
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (message instanceof ActiveMQTextMessage) {
                    try {
                        System.out.println("get message ----------------- ");
                        System.out.println("receive: " + ((ActiveMQTextMessage) message).getText());
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }
    ```
