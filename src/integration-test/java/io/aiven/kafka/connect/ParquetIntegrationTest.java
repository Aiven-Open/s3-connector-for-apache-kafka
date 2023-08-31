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
import org.apache.kafka.common.serialization.ByteArraySerializer;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
final class ParquetIntegrationTest implements IntegrationBase {

    private static final String S3_ACCESS_KEY_ID = "test-key-id0";

    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";

    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static final String CONNECTOR_NAME = "aiven-s3-sink-connector";

    private static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static String s3Endpoint;
    private static BucketAccessor testBucketAccessor;
    private static File pluginDir;
    @TempDir
    Path tmpDir;

    @Container
    public static final LocalStackContainer LOCALSTACK = IntegrationBase.createS3Container();
    @Container
    private static final KafkaContainer KAFKA = IntegrationBase.createKafkaContainer();

    private AdminClient adminClient;
    private KafkaProducer<byte[], byte[]> producer;
    private ConnectRunner connectRunner;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        final AmazonS3 s3 = IntegrationBase.createS3Client(LOCALSTACK);
        s3Endpoint = LOCALSTACK.getEndpoint().toString();
        testBucketAccessor = new BucketAccessor(s3, TEST_BUCKET_NAME);

        pluginDir = IntegrationBase.getPluginDir();
        IntegrationBase.extractConnectorPlugin(pluginDir);

        IntegrationBase.waitForRunningContainer(KAFKA);
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) throws ExecutionException, InterruptedException {
        testBucketAccessor.createBucket();

        adminClient = newAdminClient(KAFKA);
        producer = newProducer();

        final var topicName = IntegrationBase.topicName(testInfo);
        IntegrationBase.createTopics(adminClient, List.of(topicName));

        connectRunner = newConnectRunner(KAFKA, pluginDir, OFFSET_FLUSH_INTERVAL_MS);
        connectRunner.start();
    }


    @AfterEach
    void tearDown() {
        testBucketAccessor.removeBucket();
        connectRunner.stop();
        adminClient.close();
        producer.close();

        connectRunner.awaitStop();
    }

    private KafkaProducer<byte[], byte[]> newProducer() {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        return new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Test
    void allOutputFields(final TestInfo testInfo)
        throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final var compression = "none";
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", "key,value,offset,timestamp,headers");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "value-" + cnt;
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


        final var blobContents = new HashMap<String, List<GenericRecord>>();
        final List<String> expectedBlobs = Arrays.asList(
            getBlobName(topicName, 0, 0, compression),
            getBlobName(topicName, 1, 0, compression),
            getBlobName(topicName, 2, 0, compression),
            getBlobName(topicName, 3, 0, compression));
        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();

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
                final var key = "key-" + cnt;
                final var value = "value-" + cnt;
                final String blobName = getBlobName(topicName, partition, 0, compression);
                final var record = blobContents.get(blobName).get(i);

                assertThat(record.get("key")).hasToString(key);
                assertThat(record.get("value")).hasToString(value);
                assertThat(record.get("offset")).isNotNull();
                assertThat(record.get("timestamp")).isNotNull();
                assertThat(record.get("headers")).isNull();

                cnt += 1;
            }
        }
    }

    @Test
    void allOutputFieldsJsonValueAsString(final TestInfo testInfo)
        throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final var compression = "none";
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", "key,value,offset,timestamp,headers");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "{\"name\": \"name-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
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

        final var blobContents = new HashMap<String, List<GenericRecord>>();
        final List<String> expectedBlobs = Arrays.asList(
            getBlobName(topicName, 0, 0, compression),
            getBlobName(topicName, 1, 0, compression),
            getBlobName(topicName, 2, 0, compression),
            getBlobName(topicName, 3, 0, compression));
        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();

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
                final var key = "key-" + cnt;
                final var value = "{\"name\": \"name-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
                final String blobName = getBlobName(topicName, partition, 0, compression);
                final var record = blobContents.get(blobName).get(i);

                assertThat(record.get("key")).hasToString(key);
                assertThat(record.get("value")).hasToString(value);
                assertThat(record.get("offset")).isNotNull();
                assertThat(record.get("timestamp")).isNotNull();
                assertThat(record.get("headers")).isNull();

                cnt += 1;
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"true, {\"value\": {\"name\": \"%s\"}} ", "false, {\"name\": \"%s\"}"})
    void jsonValue(final String envelopeEnabled, final String expectedOutput, final TestInfo testInfo)
        throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final var compression = "none";
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.envelope", envelopeEnabled);
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectRunner.createConnector(connectorConfig);

        final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
        final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value =
                    String.format(
                        jsonMessagePattern,
                        jsonMessageSchema, "{" + "\"name\":\"user-" + cnt + "\"}"
                    );
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

        final var blobContents = new HashMap<String, List<GenericRecord>>();
        final List<String> expectedBlobs = Arrays.asList(
            getBlobName(topicName, 0, 0, compression),
            getBlobName(topicName, 1, 0, compression),
            getBlobName(topicName, 2, 0, compression),
            getBlobName(topicName, 3, 0, compression));
        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();

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
                final String blobName = getBlobName(topicName, partition, 0, compression);
                final var record = blobContents.get(blobName).get(i);
                final String expectedLine = String.format(expectedOutput, name);
                assertThat(record).hasToString(expectedLine);
                cnt += 1;
            }
        }
    }

    @Test
    void schemaChanged(final TestInfo testInfo)
        throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final var compression = "none";
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(compression), topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectRunner.createConnector(connectorConfig);

        final var jsonMessageSchema = "{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"field\":\"name\"}]}";
        final var jsonMessageNewSchema = "{\"type\":\"struct\",\"fields\":"
            + "[{\"type\":\"string\",\"field\":\"name\"}, "
            + "{\"type\":\"string\",\"field\":\"value\", \"default\": \"foo\"}]}";
        final var jsonMessagePattern = "{\"schema\": %s, \"payload\": %s}";

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final var expectedRecords = new ArrayList<String>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final String payload;
                final String value;
                if (i < 5) {
                    payload = "{" + "\"name\": \"user-" + cnt + "\"}";
                    value = String.format(jsonMessagePattern, jsonMessageSchema, payload);
                } else {
                    payload = "{" + "\"name\": \"user-" + cnt + "\", \"value\": \"value-" + cnt + "\"}";
                    value = String.format(jsonMessagePattern, jsonMessageNewSchema, payload);
                }
                expectedRecords.add(payload);
                sendFutures.add(sendMessageAsync(topicName, partition, key, value));
                cnt += 1;
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
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();

            final var records =
                ParquetUtils.readRecords(
                    tmpDir.resolve(Paths.get(blobName)),
                    testBucketAccessor.readBytes(blobName)
                );
            blobContents.addAll(records.stream().map(r -> r.get("value").toString()).collect(Collectors.toList()));
        }
        assertThat(blobContents).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    private Map<String, String> awsSpecificConfig(final Map<String, String> config, final String topicName) {
        return awsSpecificConfig(config, List.of(topicName));
    }

    private Map<String, String> awsSpecificConfig(final Map<String, String> config, final List<String> topicNames) {
        config.put("connector.class", AivenKafkaConnectS3SinkConnector.class.getName());
        config.put("aws.access.key.id", S3_ACCESS_KEY_ID);
        config.put("aws.secret.access.key", S3_SECRET_ACCESS_KEY);
        config.put("aws.s3.endpoint", s3Endpoint);
        config.put("aws.s3.bucket.name", TEST_BUCKET_NAME);
        config.put("topics", String.join(",", topicNames));
        return config;
    }

    private Map<String, String> basicConnectorConfig(final String compression) {
        final Map<String, String> config = new HashMap<>();
        config.put("name", CONNECTOR_NAME);
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", "1");
        config.put("file.compression.type", compression);
        config.put("format.output.type", "parquet");
        return config;
    }

    private Future<RecordMetadata> sendMessageAsync(final String topicName,
                                                    final int partition,
                                                    final String key,
                                                    final String value) {
        final ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(
            topicName, partition,
            key == null ? null : key.getBytes(),
            value == null ? null : value.getBytes());
        return producer.send(msg);
    }

    private String getBlobName(final String topicName, final int partition, final int startOffset,
                               final String compression) {
        final String result = String.format("%s-%d-%d", topicName, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

}
