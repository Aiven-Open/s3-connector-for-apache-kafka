/*
 * Copyright 2021 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.SchemaRegistryContainer;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class AvroParquetIntegrationTest implements IntegrationBase {
    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static final String CONNECTOR_NAME = "aiven-s3-sink-connector";
    private static final String COMMON_PREFIX = "s3-connector-for-apache-kafka-test-";
    private static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static String s3Endpoint;
    private static String s3Prefix;
    private static BucketAccessor testBucketAccessor;
    private static File pluginDir;

    @Container
    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();
    @Container
    private static final KafkaContainer KAFKA = IntegrationBase.createKafkaContainer();
    @Container
    private static final SchemaRegistryContainer SCHEMA_REGISTRY = new SchemaRegistryContainer(KAFKA);
    private AdminClient adminClient;
    private KafkaProducer<String, GenericRecord> producer;
    private ConnectRunner connectRunner;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX
            + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final AmazonS3 s3 = IntegrationBase.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3, TEST_BUCKET_NAME);
        testBucketAccessor.createBucket();

        pluginDir = IntegrationBase.getPluginDir();
        IntegrationBase.extractConnectorPlugin(pluginDir);

        IntegrationBase.waitForRunningContainer(KAFKA);
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) throws ExecutionException, InterruptedException {
        adminClient = newAdminClient(KAFKA);
        producer = newProducer();

        final var topicName = IntegrationBase.topicName(testInfo);
        IntegrationBase.createTopics(adminClient, List.of(topicName));

        connectRunner = newConnectRunner(KAFKA, pluginDir, OFFSET_FLUSH_INTERVAL_MS);
        connectRunner.start();
    }

    @AfterEach
    final void tearDown() {
        connectRunner.stop();
        adminClient.close();
        producer.close();

        connectRunner.awaitStop();
    }

    @Test
    final void allOutputFields(@TempDir final Path tmpDir, final TestInfo testInfo)
        throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final String compression = "none";
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", "key,value,offset,timestamp,headers");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectRunner.createConnector(connectorConfig);

        final Schema valueSchema =
            SchemaBuilder.record("value")
                        .fields()
                        .name("name").type().stringType().noDefault()
                        .name("value").type().stringType().noDefault()
                        .endRecord();

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final GenericRecord value = new GenericData.Record(valueSchema);
                value.put("name", "user-" + cnt);
                value.put("value", "value-" + cnt);
                cnt += 1;
                sendFutures.add(sendMessageAsync(topicName, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = List.of(
            getBlobName(topicName, 0, 0, compression),
            getBlobName(topicName, 1, 0, compression),
            getBlobName(topicName, 2, 0, compression),
            getBlobName(topicName, 3, 0, compression));
        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            assertTrue(testBucketAccessor.doesObjectExist(blobName));
            final var records =
                    ParquetUtils.readRecords(
                            tmpDir.resolve(Paths.get(blobName)),
                            testBucketAccessor.readBytes(blobName)
                    );
            blobContents.put(blobName, records);
        }

        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var name = "user-" + cnt;
                final var value = "value-" + cnt;
                final String blobName = getBlobName(topicName, partition, 0, "none");
                final GenericRecord record = blobContents.get(blobName).get(i);
                final var expectedKey = "key-" + cnt;
                final var expectedValue = "{\"name\": \"" + name + "\", \"value\": \"" + value + "\"}";
                assertEquals(expectedKey, record.get("key").toString());
                assertEquals(expectedValue, record.get("value").toString());
                assertNotNull(record.get("offset"));
                assertNotNull(record.get("timestamp"));
                assertNull(record.get("headers"));
                cnt += 1;
            }
        }
    }

    @Test
    final void valueComplexType(@TempDir final Path tmpDir, final TestInfo testInfo)
        throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final String compression = "none";
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectRunner.createConnector(connectorConfig);

        final Schema valueSchema =
            SchemaBuilder.record("value")
                        .fields()
                        .name("name").type().stringType().noDefault()
                        .name("value").type().stringType().noDefault()
                        .endRecord();

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final GenericRecord value = new GenericData.Record(valueSchema);
                value.put("name", "user-" + cnt);
                value.put("value", "value-" + cnt);
                cnt += 1;
                sendFutures.add(sendMessageAsync(topicName, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = List.of(
            getBlobName(topicName, 0, 0, compression),
            getBlobName(topicName, 1, 0, compression),
            getBlobName(topicName, 2, 0, compression),
            getBlobName(topicName, 3, 0, compression));
        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final var records =
                    ParquetUtils.readRecords(
                            tmpDir.resolve(Paths.get(blobName)),
                            testBucketAccessor.readBytes(blobName)
                    );
            blobContents.put(blobName, records);
        }
        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var name = "user-" + cnt;
                final var value = "value-" + cnt;
                final String blobName = getBlobName(topicName, partition, 0, "none");
                final var record = blobContents.get(blobName).get(i);
                final var avroRecord = (GenericRecord) record.get("value");
                assertEquals(name, avroRecord.get("name").toString());
                assertEquals(value, avroRecord.get("value").toString());
                cnt += 1;
            }
        }
    }

    @Test
    final void schemaChanged(@TempDir final Path tmpDir, final TestInfo testInfo)
        throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final String compression = "none";
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectRunner.createConnector(connectorConfig);

        final Schema valueSchema =
            SchemaBuilder.record("value")
                        .fields()
                        .name("name").type().stringType().noDefault()
                        .name("value").type().stringType().noDefault()
                        .endRecord();

        final Schema newValueSchema =
                SchemaBuilder.record("value")
                        .fields()
                        .name("name").type().stringType().noDefault()
                        .name("value").type().stringType().noDefault()
                        .name("blocked").type().booleanType().booleanDefault(false)
                        .endRecord();

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        final var expectedRecords = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final GenericRecord value;
                if (i < 5) {
                    value = new GenericData.Record(valueSchema);
                    value.put("name", "user-" + cnt);
                    value.put("value", "value-" + cnt);
                } else {
                    value = new GenericData.Record(newValueSchema);
                    value.put("name", "user-" + cnt);
                    value.put("value", "value-" + cnt);
                    value.put("blocked", true);
                }
                expectedRecords.add(value.toString());
                cnt += 1;
                sendFutures.add(sendMessageAsync(topicName, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = Arrays.asList(
            getBlobName(topicName, 0, 0, compression),
            getBlobName(topicName, 0, 5, compression),
            getBlobName(topicName, 1, 0, compression),
            getBlobName(topicName, 1, 5, compression),
            getBlobName(topicName, 2, 0, compression),
            getBlobName(topicName, 2, 5, compression),
            getBlobName(topicName, 3, 0, compression),
            getBlobName(topicName, 3, 5, compression)
        );
        final var blobContents = new ArrayList<String>();
        for (final String blobName : expectedBlobs) {
            final var records =
                    ParquetUtils.readRecords(
                            tmpDir.resolve(Paths.get(blobName)),
                            testBucketAccessor.readBytes(blobName)
                    );
            blobContents.addAll(records.stream().map(r -> r.get("value").toString()).collect(Collectors.toList()));
        }
        assertIterableEquals(
                expectedRecords.stream().sorted().collect(Collectors.toList()),
                blobContents.stream().sorted().collect(Collectors.toList())
        );
    }

    private KafkaProducer<String, GenericRecord> newProducer() {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", SCHEMA_REGISTRY.getSchemaRegistryUrl());
        return new KafkaProducer<>(producerProps);
    }

    private Future<RecordMetadata> sendMessageAsync(final String topicName,
                                                    final int partition,
                                                    final String key,
                                                    final GenericRecord value) {
        final ProducerRecord<String, GenericRecord> msg = new ProducerRecord<>(
                topicName, partition, key, value);
        return producer.send(msg);
    }

    private Map<String, String> basicConnectorConfig(final String compression) {
        final Map<String, String> config = new HashMap<>();
        config.put("name", CONNECTOR_NAME);
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", SCHEMA_REGISTRY.getSchemaRegistryUrl());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", SCHEMA_REGISTRY.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        config.put("file.compression.type", compression);
        config.put("format.output.type", "parquet");
        return config;
    }

    private Map<String, String> awsSpecificConfig(final Map<String, String> config, final String topicName) {
        config.put("connector.class", AivenKafkaConnectS3SinkConnector.class.getName());
        config.put("aws.access.key.id", S3_ACCESS_KEY_ID);
        config.put("aws.secret.access.key", S3_SECRET_ACCESS_KEY);
        config.put("aws.s3.endpoint", s3Endpoint);
        config.put("aws.s3.bucket.name", TEST_BUCKET_NAME);
        config.put("aws.s3.prefix", s3Prefix);
        config.put("topics", topicName);
        config.put("key.converter.schema.registry.url", SCHEMA_REGISTRY.getSchemaRegistryUrl());
        config.put("value.converter.schema.registry.url", SCHEMA_REGISTRY.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        return config;
    }

    // WARN: different from GCS
    private String getBlobName(final String topicName, final int partition, final int startOffset,
                               final String compression) {
        final String result = String.format("%s%s-%d-%020d", s3Prefix, topicName, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

}
