/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.jcloud;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.testcontainers.containers.PulsarContainer.BROKER_HTTP_PORT;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.io.jcloud.container.PulsarContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 * Start / stop a Pulsar cluster.
 */
@Slf4j
public abstract class PulsarTestBase {

    protected static PulsarContainer pulsarService;
    protected static Network network = Network.newNetwork();

    protected static String serviceUrl;

    protected static String adminUrl;

    public static String getServiceUrl() {
        return serviceUrl;
    }

    public static String getAdminUrl() {
        return adminUrl;
    }

    @BeforeClass
    public static void prepare() throws Exception {

        log.info("-------------------------------------------------------------------------");
        log.info("    Starting PulsarTestBase ");
        log.info("-------------------------------------------------------------------------");


        final String pulsarImage = System.getProperty("pulsar.systemtest.image", "pulsar-io-cloud-storage:latest");
        pulsarService = new PulsarContainer(DockerImageName.parse(pulsarImage)).withFunctionsWorker()
                .withNetwork(network);
        pulsarService.waitingFor(new HttpWaitStrategy()
                .forPort(BROKER_HTTP_PORT)
                .forStatusCode(200)
                .forPath("/admin/v2/namespaces/public/default")
                .withStartupTimeout(Duration.of(40, SECONDS)));
        pulsarService.start();
        pulsarService.followOutput(new Slf4jLogConsumer(log));
        serviceUrl = pulsarService.getPulsarBrokerUrl();
        adminUrl = pulsarService.getHttpServiceUrl();

        log.info("-------------------------------------------------------------------------");
        log.info("Successfully started pulsar service at cluster " + pulsarService.getContainerName());
        log.info("-------------------------------------------------------------------------");

    }

    @AfterClass
    public static void shutDownServices() throws Exception {
        log.info("-------------------------------------------------------------------------");
        log.info("    Shut down PulsarTestBase ");
        log.info("-------------------------------------------------------------------------");

        if (pulsarService != null) {
            pulsarService.stop();
        }

        log.info("-------------------------------------------------------------------------");
        log.info("    PulsarTestBase finished");
        log.info("-------------------------------------------------------------------------");
    }

    public static <T> List<MessageId> sendTypedMessages(
            String topic,
            SchemaType type,
            List<T> messages,
            Optional<Integer> partition) throws PulsarClientException {

        return sendTypedMessages(topic, type, messages, partition, null);
    }

    @SuppressWarnings("unchecked")
    public static <T> List<MessageId> sendTypedMessages(
            String topic,
            SchemaType type,
            List<T> messages,
            Optional<Integer> partition,
            Class<T> tClass) throws PulsarClientException {

        String topicName;
        if (partition.isPresent()) {
            topicName = topic + TopicName.PARTITIONED_TOPIC_SUFFIX + partition.get();
        } else {
            topicName = topic;
        }

        Producer<T> producer = null;
        PulsarClient client = null;
        List<MessageId> mids = new ArrayList<>();

        try {
            client = PulsarClient.builder().serviceUrl(getServiceUrl()).build();

            switch (type) {
                case BOOLEAN:
                    producer = (Producer<T>) client.newProducer(Schema.BOOL).topic(topicName).create();
                    break;
                case BYTES:
                    producer = (Producer<T>) client.newProducer(Schema.BYTES).topic(topicName).create();
                    break;
                case DATE:
                    producer = (Producer<T>) client.newProducer(Schema.DATE).topic(topicName).create();
                    break;
                case STRING:
                    producer = (Producer<T>) client.newProducer(Schema.STRING).topic(topicName).create();
                    break;
                case TIMESTAMP:
                    producer = (Producer<T>) client.newProducer(Schema.TIMESTAMP).topic(topicName).create();
                    break;
                case INT8:
                    producer = (Producer<T>) client.newProducer(Schema.INT8).topic(topicName).create();
                    break;
                case DOUBLE:
                    producer = (Producer<T>) client.newProducer(Schema.DOUBLE).topic(topicName).create();
                    break;
                case FLOAT:
                    producer = (Producer<T>) client.newProducer(Schema.FLOAT).topic(topicName).create();
                    break;
                case INT32:
                    producer = (Producer<T>) client.newProducer(Schema.INT32).topic(topicName).create();
                    break;
                case INT16:
                    producer = (Producer<T>) client.newProducer(Schema.INT16).topic(topicName).create();
                    break;
                case INT64:
                    producer = (Producer<T>) client.newProducer(Schema.INT64).topic(topicName).create();
                    break;
                case AVRO:
                    producer = (Producer<T>) client.newProducer(Schema.AVRO(tClass)).topic(topicName).create();
                    break;
                case JSON:
                    producer = (Producer<T>) client.newProducer(Schema.JSON(tClass)).topic(topicName).create();
                    break;

                default:
                    throw new NotImplementedException("Unsupported type " + type);
            }

            for (T message : messages) {
                MessageId mid = producer.send(message);
                log.info("Sent {} of mid: {}", message.toString(), mid.toString());
                mids.add(mid);
            }

        } catch (Exception e) {
            log.error("send message failed", e);
        } finally {
            producer.flush();
            producer.close();
            client.close();
        }
        return mids;
    }

    public static <T> void consumerMessages(String topic,
                                            Schema<T> schema,
                                            java.util.function.Consumer<Message<T>> handler,
                                            int count,
                                            long timeout) throws Exception {
        PulsarClient client = null;
        Consumer<T> consumer = null;
        try {
            client = PulsarClient.builder().serviceUrl(getServiceUrl()).build();

            consumer = client.newConsumer(schema)
                    .topic(topic)
                    .subscriptionName("test")
                    .subscribe();
            int receiveCount = 0;
            while (receiveCount < count) {
                final CompletableFuture<Message<T>> receiveAsync = consumer.receiveAsync();
                final Message<T> message = receiveAsync.get(timeout, TimeUnit.MILLISECONDS);
                handler.accept(message);
                consumer.acknowledge(message);
                receiveCount += 1;
            }
        } finally {
            IOUtils.closeQuietly(consumer);
            IOUtils.closeQuietly(client);
        }
    }
}
