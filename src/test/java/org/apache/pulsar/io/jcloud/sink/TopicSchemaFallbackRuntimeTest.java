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
package org.apache.pulsar.io.jcloud.sink;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.io.jcloud.PulsarContainer;
import org.apache.pulsar.io.jcloud.format.ParquetInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

/** Actual packaged connector in a broker's IO worker, with pooled messages, redelivery, DLQ and MinIO. */
@EnabledIfSystemProperty(named = "topicSchemaFallback.integration", matches = "true")
public class TopicSchemaFallbackRuntimeTest {
    private static final String TOPIC = "persistent://public/default/iot-runtime";
    private static final String DLQ = TOPIC + "-dlq";
    private static final String BUCKET = "fallback-test";

    @Test
    public void packagedIoWorkerParquetRedeliveryAndDlq() throws Exception {
        Path nar = Path.of(System.getProperty("topicSchemaFallback.nar",
                "target/pulsar-io-cloud-storage-0.0.1.nar")).toAbsolutePath();
        assertTrue(Files.isRegularFile(nar), "Build the NAR with mvn package -DskipTests first");
        try (Network network = Network.newNetwork();
             GenericContainer<?> minio = new GenericContainer<>(DockerImageName.parse(
                     "minio/minio:RELEASE.2024-10-13T13-34-11Z"))
                     .withNetwork(network).withNetworkAliases("minio").withExposedPorts(9000)
                     .withEnv("MINIO_ROOT_USER", "minioadmin").withEnv("MINIO_ROOT_PASSWORD", "minioadmin")
                     .withCommand("server", "/data").waitingFor(Wait.forHttp("/minio/health/ready"));
             PulsarContainer pulsar = new PulsarContainer(DockerImageName.parse(
                     System.getProperty("pulsar.systemtest.image", "apachepulsar/pulsar:4.2.1")))
                     .withFunctionsWorker().withNetwork(network).withNetworkAliases("pulsar")
                     .withEnv("PULSAR_MEM", "-Xms256m -Xmx1024m -XX:MaxDirectMemorySize=512m")
                     .withStartupTimeout(Duration.ofMinutes(3))) {
            minio.start();
            pulsar.start();
            try {
                runScenario(pulsar, minio, nar);
            } catch (Throwable failure) {
                Files.writeString(Path.of("target/topic-schema-runtime-broker.log"), pulsar.getLogs());
                ExecResult instanceLogs = pulsar.execInContainer("sh", "-c",
                        "cat /pulsar/logs/functions/public/default/pinned-json/*.log");
                Files.writeString(Path.of("target/topic-schema-runtime-instances.log"), instanceLogs.getStdout());
                throw failure;
            }
        }
    }

    private void runScenario(PulsarContainer pulsar, GenericContainer<?> minio, Path nar) throws Exception {
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build();
             PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
             S3Client s3 = S3Client.builder()
                     .endpointOverride(java.net.URI.create(
                             "http://" + minio.getHost() + ":" + minio.getMappedPort(9000)))
                     .forcePathStyle(true).region(Region.US_EAST_1)
                     .credentialsProvider(StaticCredentialsProvider.create(
                             AwsBasicCredentials.create("minioadmin", "minioadmin"))).build()) {
            s3.createBucket(request -> request.bucket(BUCKET));
            minioCommand(minio, "alias", "set", "local", "http://localhost:9000", "minioadmin", "minioadmin");
            minioCommand(minio, "admin", "user", "add", "local", "sinkwriter", "test-sink-writer-secret");
            minioCommand(minio, "admin", "policy", "attach", "local", "readwrite", "--user", "sinkwriter");
            SchemaInfo info = schema(false);
            admin.schemas().createSchema(TOPIC, info);
            // An IP endpoint makes the AWS SDK use path addressing against MinIO.
            String endpoint = "http://" + minio.getContainerInfo().getNetworkSettings().getNetworks()
                    .values().iterator().next().getIpAddress() + ":9000";
            Map<String, Object> connector = new HashMap<>(Map.of("provider", "s3v2", "bucket", BUCKET,
                    "region", "us-east-1", "endpoint", endpoint, "accessKeyId", "sinkwriter",
                    "secretAccessKey", "test-sink-writer-secret", "formatType", "parquet",
                    "partitionerType", "partition",
                    "batchSize", 2, "batchTimeMs", 500));
            connector.put("topicSchemaFallback", Map.of("enabled", true, "topicVersions", Map.of(TOPIC, 0)));
            connector.put("schemalessDecodeFailureAction", "REDELIVER");
            Map<String, Object> runtime = new HashMap<>(Map.of("tenant", "public", "namespace", "default",
                    "name", "pinned-json", "parallelism", 2, "processingGuarantees", "ATLEAST_ONCE",
                    "autoAck", false, "inputSpecs", Map.of(TOPIC, Map.of("poolMessages", true)),
                    "maxMessageRetries", 2, "deadLetterTopic", DLQ, "negativeAckRedeliveryDelayMs", 100));
            runtime.put("configs", connector);
            pulsar.copyFileToContainer(MountableFile.forHostPath(nar), "/tmp/cloud-storage.nar");
            pulsar.copyFileToContainer(Transferable.of(new ObjectMapper().writeValueAsBytes(runtime)),
                    "/tmp/sink.json");
            command(pulsar, "sinks", "create", "--archive", "/tmp/cloud-storage.nar",
                    "--sink-config-file", "/tmp/sink.json", "--max-redeliver-count", "2", "--dead-letter-topic", DLQ);
            awaitStatus(pulsar, 0);
            byte[] malformed = "{\"id\":\"bad\"}".getBytes(StandardCharsets.UTF_8);
            try (Consumer<byte[]> dlq = client.newConsumer(Schema.BYTES).topic(DLQ).subscriptionName("verify-dlq")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
                 Producer<byte[]> bytes = client.newProducer(Schema.BYTES).topic(TOPIC).enableBatching(false).create();
                 Producer<GenericRecord> typed = client.newProducer(Schema.generic(info))
                         .topic(TOPIC).enableBatching(false).create()) {
                bytes.newMessage().value("{\"id\":1}".getBytes(StandardCharsets.UTF_8)).send();
                bytes.newMessage().key("iot-key").property("source", "iot").eventTime(123L).value(malformed).send();
                bytes.newMessage().value("{\"id\":3}".getBytes(StandardCharsets.UTF_8)).send();
                typed.send(Schema.generic(info).decode("{\"id\":4}".getBytes(StandardCharsets.UTF_8)));
                Message<byte[]> dead = dlq.receive(60, TimeUnit.SECONDS);
                assertNotNull(dead, "Bad record must exhaust the worker's retry count and reach its DLQ");
                assertArrayEquals(malformed, dead.getValue());
                assertEquals("iot", dead.getProperty("source"));
                assertEquals("iot-key", dead.getKey());
                assertEquals(123L, dead.getEventTime());
                assertTrue(dead.getProperties().containsKey("ORIGIN_MESSAGE_ID"));
                dlq.acknowledge(dead);
                assertNull(dlq.receive(2, TimeUnit.SECONDS), "Good records must not enter the DLQ");
                awaitRows(s3, Set.of(1, 3, 4));
                // Three good deliveries plus initial bad delivery and exactly two bad redeliveries.
                awaitStatus(pulsar, 6);
                Files.writeString(Path.of("target/topic-schema-runtime-retry-status.json"),
                        command(pulsar, "sinks", "status", "--name", "pinned-json"));
                // A newly registered schema must not change the explicit v0 interpretation after restart.
                admin.schemas().createSchema(TOPIC, schema(true));
                command(pulsar, "sinks", "restart", "--name", "pinned-json");
                awaitStatus(pulsar, 0);
                bytes.send("{\"id\":5}".getBytes(StandardCharsets.UTF_8));
                awaitRows(s3, Set.of(1, 3, 4, 5));
                assertNull(dlq.receive(2, TimeUnit.SECONDS));
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    long unacked = admin.topics().getStats(TOPIC).getSubscriptions().values().stream()
                            .mapToLong(subscription -> subscription.getUnackedMessages()).sum();
                    assertEquals(0, unacked);
                });
                minioCommand(minio, "admin", "user", "disable", "local", "sinkwriter");
                try {
                    bytes.send("{\"id\":6}".getBytes(StandardCharsets.UTF_8));
                    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                        ExecResult logs = pulsar.execInContainer("sh", "-c",
                                "cat /pulsar/logs/functions/public/default/pinned-json/*.log");
                        assertTrue(logs.getStdout().contains("explicit recovery is required (fatal=false)"));
                    });
                    awaitStatus(pulsar, 2);
                    assertNull(dlq.receive(3, TimeUnit.SECONDS), "Storage outage must not send good data to the DLQ");
                    awaitRows(s3, Set.of(1, 3, 4, 5));
                    assertTrue(admin.topics().getStats(TOPIC).getSubscriptions().values().stream()
                            .anyMatch(subscription -> subscription.getUnackedMessages() > 0));
                } finally {
                    minioCommand(minio, "admin", "user", "enable", "local", "sinkwriter");
                }
                command(pulsar, "sinks", "restart", "--name", "pinned-json");
                awaitRows(s3, Set.of(1, 3, 4, 5, 6));
                assertNull(dlq.receive(2, TimeUnit.SECONDS));
                String status = command(pulsar, "sinks", "status", "--name", "pinned-json");
                Files.writeString(Path.of("target/topic-schema-runtime-status.json"), status);
            } finally {
                command(pulsar, "sinks", "delete", "--name", "pinned-json");
            }
        }
    }

    private static SchemaInfo schema(boolean evolved) {
        org.apache.avro.Schema avro = evolved
                ? SchemaBuilder.record("Event").fields().requiredInt("id").optionalString("newField").endRecord()
                : SchemaBuilder.record("Event").fields().requiredInt("id").endRecord();
        return SchemaInfo.builder().type(SchemaType.JSON).name("Event")
                .schema(avro.toString().getBytes(StandardCharsets.UTF_8)).properties(Map.of()).build();
    }

    private static void awaitStatus(PulsarContainer pulsar, int expectedDeliveries) {
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            JsonNode status = new ObjectMapper().readTree(
                    command(pulsar, "sinks", "status", "--name", "pinned-json"));
            assertEquals(2, status.get("numRunning").asInt());
            long deliveries = 0;
            for (JsonNode instance : status.get("instances")) {
                JsonNode running = instance.get("status");
                assertEquals(0, running.get("numRestarts").asLong());
                assertEquals(0, running.get("numSystemExceptions").asLong());
                assertEquals(0, running.get("numSinkExceptions").asLong());
                deliveries += running.get("numReadFromPulsar").asLong();
            }
            assertEquals(expectedDeliveries, deliveries);
        });
    }

    private static void awaitRows(S3Client s3, Set<Integer> expected) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            Set<Integer> actual = new HashSet<>();
            for (software.amazon.awssdk.services.s3.model.S3Object object : s3.listObjectsV2(
                    ListObjectsV2Request.builder().bucket(BUCKET).build()).contents()) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                bytes.write(s3.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(object.key()).build())
                        .asByteArray());
                try (ParquetReader<org.apache.avro.generic.GenericRecord> reader = AvroParquetReader
                        .<org.apache.avro.generic.GenericRecord>builder(new ParquetInputFile(object.key(), bytes))
                        .withDataModel(GenericData.get()).build()) {
                    org.apache.avro.generic.GenericRecord row;
                    while ((row = reader.read()) != null) {
                        actual.add((Integer) row.get("id"));
                        // Pin v0 has exactly one field, including after the registry advances to v1.
                        assertEquals(1, row.getSchema().getFields().size());
                    }
                }
            }
            assertEquals(expected, actual);
        });
    }

    private static void minioCommand(GenericContainer<?> minio, String... args) throws Exception {
        List<String> command = new java.util.ArrayList<>(List.of("/usr/bin/mc"));
        command.addAll(List.of(args));
        ExecResult result = minio.execInContainer(command.toArray(new String[0]));
        assertEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
    }

    private static String command(PulsarContainer pulsar, String... args) throws Exception {
        List<String> command = new java.util.ArrayList<>(List.of("/pulsar/bin/pulsar-admin"));
        command.addAll(List.of(args));
        ExecResult result = pulsar.execInContainer(command.toArray(new String[0]));
        assertEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
        return result.getStdout();
    }
}
