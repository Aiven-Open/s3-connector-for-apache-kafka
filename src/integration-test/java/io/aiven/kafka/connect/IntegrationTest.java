/*
 * Copyright 2020 Aiven Oy
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
import java.time.ZoneId;
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
import org.apache.kafka.common.TopicPartition;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.testutils.IndexesToString;
import io.aiven.kafka.connect.s3.testutils.KeyValueGenerator;
import io.aiven.kafka.connect.s3.testutils.KeyValueMessage;

import com.amazonaws.services.s3.AmazonS3;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static com.amazonaws.SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
final class IntegrationTest implements IntegrationBase {
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
    private AdminClient adminClient;
    private KafkaProducer<byte[], byte[]> producer;
    private ConnectRunner connectRunner;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX
            + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

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
        final var topics = List.of(topicName);
        IntegrationBase.createTopics(adminClient, topics);

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

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    void basicTest(final String compression, final TestInfo testInfo)
        throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("aws.s3.prefix", s3Prefix);
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final IndexesToString keyGen = (partition, epoch, currIdx) -> "key-" + currIdx;
        final IndexesToString valueGen = (partition, epoch, currIdx) -> "value-" + currIdx;

        for (final KeyValueMessage msg : new KeyValueGenerator(4, 10, keyGen, valueGen)) {
            sendFutures.add(sendMessageAsync(producer, topicName, msg.partition, msg.key, msg.value));
        }

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = Arrays.asList(
            getOldBlobName(topicName, 0, 0, compression),
            getOldBlobName(topicName, 1, 0, compression),
            getOldBlobName(topicName, 2, 0, compression),
            getOldBlobName(topicName, 3, 0, compression));
        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            blobContents.put(blobName,
                testBucketAccessor.readAndDecodeLines(blobName, compression, 0, 1).stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.toList())
            );
        }

        for (final KeyValueMessage msg : new KeyValueGenerator(4, 10, keyGen, valueGen)) {
            final String blobName = getOldBlobName(topicName, msg.partition, 0, compression);

            assertThat(blobContents.get(blobName).get(msg.epoch)).isEqualTo(msg.key + "," + msg.value);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    void groupByTimestampVariable(final String compression, final TestInfo testInfo) throws ExecutionException,
        InterruptedException,
        IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put(
            "file.name.template",
            s3Prefix + "{{topic}}-{{partition}}-{{start_offset}}-"
                + "{{timestamp:unit=yyyy}}-{{timestamp:unit=MM}}-{{timestamp:unit=dd}}"
        );
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        sendFutures.add(sendMessageAsync(producer, topicName, 0, "key-0", "value-0"));
        sendFutures.add(sendMessageAsync(producer, topicName, 0, "key-1", "value-1"));
        sendFutures.add(sendMessageAsync(producer, topicName, 0, "key-2", "value-2"));
        sendFutures.add(sendMessageAsync(producer, topicName, 1, "key-3", "value-3"));
        sendFutures.add(sendMessageAsync(producer, topicName, 3, "key-4", "value-4"));

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final Map<String, String[]> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(
            getTimestampBlobName(topicName, 0, 0),
            new String[] {"key-0,value-0", "key-1,value-1", "key-2,value-2"}
        );
        expectedBlobsAndContent.put(
            getTimestampBlobName(topicName, 1, 0),
            new String[] {"key-3,value-3"}
        );
        expectedBlobsAndContent.put(
            getTimestampBlobName(topicName, 3, 0),
            new String[] {"key-4,value-4"}
        );

        final List<String> expectedBlobs =
            expectedBlobsAndContent.keySet().stream().sorted().collect(Collectors.toList());
        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        for (final String blobName : expectedBlobs) {
            final List<String> blobContent =
                testBucketAccessor.readAndDecodeLines(blobName, compression, 0, 1).stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.toList());
            assertThat(blobContent).containsExactlyInAnyOrder(expectedBlobsAndContent.get(blobName));
        }
    }

    private String getTimestampBlobName(final String topicName, final int partition, final int startOffset) {
        final ZonedDateTime time = ZonedDateTime.now(ZoneId.of("UTC"));
        return String.format(
            "%s%s-%d-%d-%s-%s-%s",
            s3Prefix,
            topicName,
            partition,
            startOffset,
            time.format(DateTimeFormatter.ofPattern("yyyy")),
            time.format(DateTimeFormatter.ofPattern("MM")),
            time.format(DateTimeFormatter.ofPattern("dd"))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    void oneFilePerRecordWithPlainValues(final String compression, final TestInfo testInfo)
        throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.max.records", "1");
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        sendFutures.add(sendMessageAsync(producer, topicName, 0, "key-0", "value-0"));
        sendFutures.add(sendMessageAsync(producer, topicName, 0, "key-1", "value-1"));
        sendFutures.add(sendMessageAsync(producer, topicName, 0, "key-2", "value-2"));
        sendFutures.add(sendMessageAsync(producer, topicName, 1, "key-3", "value-3"));
        sendFutures.add(sendMessageAsync(producer, topicName, 3, "key-4", "value-4"));

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final Map<String, String> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getNewBlobName(topicName, 0, 0, compression), "value-0");
        expectedBlobsAndContent.put(getNewBlobName(topicName, 0, 1, compression), "value-1");
        expectedBlobsAndContent.put(getNewBlobName(topicName, 0, 2, compression), "value-2");
        expectedBlobsAndContent.put(getNewBlobName(topicName, 1, 0, compression), "value-3");
        expectedBlobsAndContent.put(getNewBlobName(topicName, 3, 0, compression), "value-4");
        final List<String> expectedBlobs =
            expectedBlobsAndContent.keySet().stream().sorted().collect(Collectors.toList());


        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        for (final String blobName : expectedBlobsAndContent.keySet()) {
            assertThat(testBucketAccessor.readLines(blobName, compression).get(0))
                .isEqualTo(expectedBlobsAndContent.get(blobName));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    void groupByKey(final String compression, final TestInfo testInfo)
        throws ExecutionException, InterruptedException, IOException {
        final var topicName0 = IntegrationBase.topicName(testInfo);
        final var topicName1 = IntegrationBase.topicName(testInfo) + "_1";
        IntegrationBase.createTopics(adminClient, List.of(topicName1));
        final Map<String, String> connectorConfig =
            awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME), List.of(topicName0, topicName1));
        final CompressionType compressionType = CompressionType.forName(compression);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("file.name.template", s3Prefix + "{{key}}" + compressionType.extension());
        connectRunner.createConnector(connectorConfig);

        final Map<TopicPartition, List<String>> keysPerTopicPartition = new HashMap<>();
        keysPerTopicPartition.put(
            new TopicPartition(topicName0, 0), Arrays.asList("key-0", "key-1", "key-2", "key-3"));
        keysPerTopicPartition.put(new TopicPartition(topicName0, 1), Arrays.asList("key-4", "key-5", "key-6"));
        keysPerTopicPartition.put(new TopicPartition(topicName1, 0), Arrays.asList(null, "key-7"));
        keysPerTopicPartition.put(new TopicPartition(topicName1, 1), Arrays.asList("key-8"));

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final Map<String, String> lastValuePerKey = new HashMap<>();
        final int numValuesPerKey = 10;
        final int cntMax = 10 * numValuesPerKey;
        int cnt = 0;
        outer:
        while (true) {
            for (final TopicPartition tp : keysPerTopicPartition.keySet()) {
                for (final String key : keysPerTopicPartition.get(tp)) {
                    final String value = "value-" + cnt;
                    cnt += 1;
                    sendFutures.add(sendMessageAsync(producer, tp.topic(), tp.partition(), key, value));
                    lastValuePerKey.put(key, value);
                    if (cnt >= cntMax) {
                        break outer;
                    }
                }
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = keysPerTopicPartition.values().stream()
            .flatMap(keys -> keys.stream().map(k -> getKeyBlobName(k, compression)))
            .collect(Collectors.toList());

        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        for (final String blobName : expectedBlobs) {
            final String blobContent = testBucketAccessor.readAndDecodeLines(blobName, compression, 0, 1).stream()
                .map(fields -> String.join(",", fields))
                .collect(Collectors.joining());
            final String keyInBlobName = blobName.replace(s3Prefix, "")
                .replace(compressionType.extension(), "");
            final String value;
            final String expectedBlobContent;
            if (keyInBlobName.equals("null")) {
                value = lastValuePerKey.get(null);
                expectedBlobContent = String.format("%s,%s", "", value);
            } else {
                value = lastValuePerKey.get(keyInBlobName);
                expectedBlobContent = String.format("%s,%s", keyInBlobName, value);
            }
            assertThat(blobContent).isEqualTo(expectedBlobContent);
        }
    }

    @Test
    void jsonlOutputTest(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        final String compression = "none";
        final String contentType = "jsonl";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.type", contentType);
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";
                cnt += 1;

                sendFutures.add(sendMessageAsync(producer, topicName, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = Arrays.asList(
            getNewBlobName(topicName, 0, 0, compression),
            getNewBlobName(topicName, 1, 0, compression),
            getNewBlobName(topicName, 2, 0, compression),
            getNewBlobName(topicName, 3, 0, compression));
        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = new ArrayList<>(testBucketAccessor.readLines(blobName, compression));
            blobContents.put(blobName, items);
        }

        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "[{" + "\"name\":\"user-" + cnt + "\"}]";
                cnt += 1;

                final String blobName = getNewBlobName(topicName, partition, 0, "none");
                final String expectedLine = "{\"value\":" + value + ",\"key\":\"" + key + "\"}";

                assertThat(blobContents.get(blobName).get(i)).isEqualTo(expectedLine);
            }
        }
    }

    @Test
    void jsonOutput(final TestInfo testInfo) throws ExecutionException, InterruptedException, IOException {
        final var topicName = IntegrationBase.topicName(testInfo);
        final var faultyProxy = enableFaultyProxy(topicName);
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME), topicName);
        connectorConfig.put("aws.s3.endpoint", faultyProxy.baseUrl());
        final String compression = "none";
        final String contentType = "json";
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.type", contentType);
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        final int numEpochs = 10;
        final int numPartitions = 4;

        final IndexesToString keyGen = (partition, epoch, currIdx) -> "key-" + currIdx;
        final IndexesToString valueGen =
            (partition, epoch, currIdx) -> "[{" + "\"name\":\"user-" + currIdx + "\"}]";

        for (final KeyValueMessage msg : new KeyValueGenerator(numPartitions, numEpochs, keyGen, valueGen)) {
            sendFutures.add(sendMessageAsync(producer, topicName, msg.partition, msg.key, msg.value));
        }

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        while (testBucketAccessor.listObjects().size() < numPartitions) {
            Thread.sleep(OFFSET_FLUSH_INTERVAL_MS);
        }

        final List<String> expectedBlobs = Arrays.asList(
            getNewBlobName(topicName, 0, 0, compression),
            getNewBlobName(topicName, 1, 0, compression),
            getNewBlobName(topicName, 2, 0, compression),
            getNewBlobName(topicName, 3, 0, compression));
        for (final String blobName : expectedBlobs) {
            assertThat(testBucketAccessor.doesObjectExist(blobName)).isTrue();
        }

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> items = new ArrayList<>(testBucketAccessor.readLines(blobName, compression));

            assertThat(items).hasSize(numEpochs + 2);

            blobContents.put(blobName, items);
        }

        // Each blob must be a JSON array.
        for (final KeyValueMessage msg : new KeyValueGenerator(numPartitions, numEpochs, keyGen, valueGen)) {
            final String blobName = getNewBlobName(topicName, msg.partition, 0, compression);
            final List<String> blobContent = blobContents.get(blobName);

            assertThat(blobContent.get(0)).isEqualTo("[");
            assertThat(blobContent.get(blobContent.size() - 1)).isEqualTo("]");

            final String actualLine = blobContent.get(msg.epoch + 1);  // 0 is '['

            String expectedLine = "{\"value\":" + msg.value + ",\"key\":\"" + msg.key + "\"}";
            if (actualLine.endsWith(",")) {
                expectedLine += ",";
            }
            assertThat(actualLine).isEqualTo(expectedLine);
        }
    }

    private static WireMockServer enableFaultyProxy(final String topicName) {
        System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        final WireMockServer wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicHttpsPort());
        wireMockServer.start();
        wireMockServer
            .addStubMapping(WireMock.request(RequestMethod.ANY.getName(), UrlPattern.ANY)
                .willReturn(aResponse().proxiedFrom(s3Endpoint)).build());
        final String urlPathPattern = "/" + TEST_BUCKET_NAME + "/" + topicName + "([\\-0-9]+)";
        wireMockServer
            .addStubMapping(WireMock.request(RequestMethod.POST.getName(),
                    UrlPattern.fromOneOf(null, null, null, urlPathPattern))
                .inScenario("temp-error").willSetStateTo("Error")
                .willReturn(WireMock.aResponse().withStatus(400))
                .build());
        wireMockServer
            .addStubMapping(WireMock.request(RequestMethod.POST.getName(),
                    UrlPattern.fromOneOf(null, null, null, urlPathPattern))
                .inScenario("temp-error").whenScenarioStateIs("Error")
                .willReturn(aResponse().proxiedFrom(s3Endpoint))
                .build());
        return wireMockServer;
    }

    private KafkaProducer<byte[], byte[]> newProducer() {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(producerProps);
    }

    private Future<RecordMetadata> sendMessageAsync(final KafkaProducer<byte[], byte[]> producer,
                                                    final String topicName,
                                                    final int partition,
                                                    final String key,
                                                    final String value) {
        final ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(
            topicName, partition,
            key == null ? null : key.getBytes(),
            value == null ? null : value.getBytes());
        return producer.send(msg);
    }

    private Map<String, String> basicConnectorConfig(final String connectorName) {
        final Map<String, String> config = new HashMap<>();
        config.put("name", connectorName);
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", "1");
        return config;
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

    // WARN: different from GCS
    private String getOldBlobName(final String topicName, final int partition, final int startOffset,
                                  final String compression) {
        final String result = String.format("%s%s-%d-%020d", s3Prefix, topicName, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

    private String getNewBlobName(final String topicName, final int partition, final int startOffset,
                                  final String compression) {
        final String result = String.format("%s-%d-%d", topicName, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

    protected String getKeyBlobName(final String key, final String compression) {
        final String result = String.format("%s%s", s3Prefix, key);
        return result + CompressionType.forName(compression).extension();
    }
}
