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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;
import io.aiven.kafka.connect.s3.testutils.IndexesToString;
import io.aiven.kafka.connect.s3.testutils.KeyValueGenerator;
import io.aiven.kafka.connect.s3.testutils.KeyValueMessage;

import cloud.localstack.Localstack;
import cloud.localstack.awssdkv1.TestUtils;
import cloud.localstack.docker.LocalstackDockerExtension;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.services.s3.AmazonS3;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = {"s3"})
@Testcontainers
final class IntegrationTest implements KafkaIntegrationBase {
    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static final String CONNECTOR_NAME = "aiven-s3-sink-connector";
    private static final String TEST_TOPIC_0 = "test-topic-0";
    private static final String TEST_TOPIC_1 = "test-topic-1";
    private static final String COMMON_PREFIX = "aiven-kafka-connect-s3-test-";
    private static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static String s3Endpoint;
    private static String s3Prefix;
    private static BucketAccessor testBucketAccessor;
    private static File pluginDir;

    @Container
    private final KafkaContainer kafka = new KafkaContainer()
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    private AdminClient adminClient;
    private KafkaProducer<byte[], byte[]> producer;
    private ConnectRunner connectRunner;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX
            + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final AmazonS3 s3 = TestUtils.getClientS3();
        s3Endpoint = Localstack.INSTANCE.getEndpointS3();
        testBucketAccessor = new BucketAccessor(s3, TEST_BUCKET_NAME);

        pluginDir = KafkaIntegrationBase.getPluginDir();
        KafkaIntegrationBase.extractConnectorPlugin(pluginDir);
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        testBucketAccessor.createBucket();

        adminClient = newAdminClient(kafka);
        producer = newProducer(kafka);

        final NewTopic newTopic0 = new NewTopic(TEST_TOPIC_0, 4, (short) 1);
        final NewTopic newTopic1 = new NewTopic(TEST_TOPIC_1, 4, (short) 1);
        adminClient.createTopics(Arrays.asList(newTopic0, newTopic1)).all().get();

        connectRunner = newConnectRunner(kafka, pluginDir, OFFSET_FLUSH_INTERVAL_MS);
        connectRunner.start();
    }

    @AfterEach
    final void tearDown() {
        testBucketAccessor.removeBucket();
        connectRunner.stop();
        adminClient.close();
        producer.close();

        connectRunner.awaitStop();
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    final void basicTest(final String compression) throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME));
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("aws.s3.prefix", s3Prefix);
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        final IndexesToString keyGen = (partition, epoch, currIdx) -> "key-" + currIdx;
        final IndexesToString valueGen = (partition, epoch, currIdx) -> "value-" + currIdx;

        for (final KeyValueMessage msg : new KeyValueGenerator(4, 10, keyGen, valueGen)) {
            sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, msg.partition, msg.key, msg.value));
        }

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = Arrays.asList(
            getOldBlobName(0, 0, compression),
            getOldBlobName(1, 0, compression),
            getOldBlobName(2, 0, compression),
            getOldBlobName(3, 0, compression));
        for (final String blobName : expectedBlobs) {
            assertTrue(testBucketAccessor.doesObjectExist(blobName));
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
            final String blobName = getOldBlobName(msg.partition, 0, compression);
            final String actualLine = blobContents.get(blobName).get(msg.epoch);
            final String expectedLine = msg.key + "," + msg.value;
            assertEquals(expectedLine, actualLine);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    final void groupByTimestampVariable(final String compression) throws ExecutionException,
                                                                         InterruptedException,
                                                                         IOException {
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME));
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put(
            "file.name.template",
            s3Prefix + "{{topic}}-{{partition}}-{{start_offset}}-"
                + "{{timestamp:unit=YYYY}}-{{timestamp:unit=MM}}-{{timestamp:unit=dd}}"
        );
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, 0, "key-0", "value-0"));
        sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, 0, "key-1", "value-1"));
        sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, 0, "key-2", "value-2"));
        sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, 1, "key-3", "value-3"));
        sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, 3, "key-4", "value-4"));

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final Map<String, String[]> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(
            getTimestampBlobName(0, 0),
            new String[]{"key-0,value-0", "key-1,value-1", "key-2,value-2"}
        );
        expectedBlobsAndContent.put(
            getTimestampBlobName(1, 0),
            new String[]{"key-3,value-3"}
        );
        expectedBlobsAndContent.put(
            getTimestampBlobName(3, 0),
            new String[]{"key-4,value-4"}
        );

        final List<String> expectedBlobs =
            expectedBlobsAndContent.keySet().stream().sorted().collect(Collectors.toList());
        for (final String blobName : expectedBlobs) {
            assertTrue(testBucketAccessor.doesObjectExist(blobName));
        }

        final Map<String, List<String>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final List<String> blobContent =
                testBucketAccessor.readAndDecodeLines(blobName, compression, 0, 1).stream()
                    .map(fields -> String.join(",", fields))
                    .collect(Collectors.toList());
            assertThat(blobContent, containsInAnyOrder(expectedBlobsAndContent.get(blobName)));

        }
    }

    private String getTimestampBlobName(final int partition, final int startOffset) {
        final ZonedDateTime time = ZonedDateTime.now(ZoneId.of("UTC"));
        return String.format(
            "%s%s-%d-%d-%s-%s-%s",
            s3Prefix,
            TEST_TOPIC_0,
            partition,
            startOffset,
            time.format(DateTimeFormatter.ofPattern("yyyy")),
            time.format(DateTimeFormatter.ofPattern("MM")),
            time.format(DateTimeFormatter.ofPattern("dd"))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    final void oneFilePerRecordWithPlainValues(final String compression)
        throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME));
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.max.records", "1");
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, 0, "key-0", "value-0"));
        sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, 0, "key-1", "value-1"));
        sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, 0, "key-2", "value-2"));
        sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, 1, "key-3", "value-3"));
        sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, 3, "key-4", "value-4"));

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final Map<String, String> expectedBlobsAndContent = new HashMap<>();
        expectedBlobsAndContent.put(getNewBlobName(0, 0, compression), "value-0");
        expectedBlobsAndContent.put(getNewBlobName(0, 1, compression), "value-1");
        expectedBlobsAndContent.put(getNewBlobName(0, 2, compression), "value-2");
        expectedBlobsAndContent.put(getNewBlobName(1, 0, compression), "value-3");
        expectedBlobsAndContent.put(getNewBlobName(3, 0, compression), "value-4");
        final List<String> expectedBlobs =
            expectedBlobsAndContent.keySet().stream().sorted().collect(Collectors.toList());


        for (final String blobName : expectedBlobs) {
            assertTrue(testBucketAccessor.doesObjectExist(blobName));
        }

        for (final String blobName : expectedBlobsAndContent.keySet()) {
            assertEquals(
                expectedBlobsAndContent.get(blobName),
                testBucketAccessor.readLines(blobName, compression).get(0)
            );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    final void groupByKey(final String compression) throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME));
        final CompressionType compressionType = CompressionType.forName(compression);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("format.output.fields", "key,value");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("file.name.template", s3Prefix + "{{key}}" + compressionType.extension());
        connectRunner.createConnector(connectorConfig);

        final Map<TopicPartition, List<String>> keysPerTopicPartition = new HashMap<>();
        keysPerTopicPartition.put(
            new TopicPartition(TEST_TOPIC_0, 0), Arrays.asList("key-0", "key-1", "key-2", "key-3"));
        keysPerTopicPartition.put(new TopicPartition(TEST_TOPIC_0, 1), Arrays.asList("key-4", "key-5", "key-6"));
        keysPerTopicPartition.put(new TopicPartition(TEST_TOPIC_1, 0), Arrays.asList(null, "key-7"));
        keysPerTopicPartition.put(new TopicPartition(TEST_TOPIC_1, 1), Arrays.asList("key-8"));

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
            assertTrue(testBucketAccessor.doesObjectExist(blobName));
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
            assertEquals(expectedBlobContent, blobContent);
        }
    }

    @Test
    final void jsonlOutputTest() throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME));
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

                sendFutures.add(sendMessageAsync(producer, TEST_TOPIC_0, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = Arrays.asList(
            getNewBlobName(0, 0, compression),
            getNewBlobName(1, 0, compression),
            getNewBlobName(2, 0, compression),
            getNewBlobName(3, 0, compression));
        for (final String blobName : expectedBlobs) {
            assertTrue(testBucketAccessor.doesObjectExist(blobName));
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

                final String blobName = getNewBlobName(partition, 0, "none");
                final String actualLine = blobContents.get(blobName).get(i);
                final String expectedLine = "{\"value\":" + value + ",\"key\":\"" + key + "\"}";
                assertEquals(expectedLine, actualLine);
            }
        }
    }

    private Map<String, String> awsSpecificConfig(final Map<String, String> config) {
        config.put("connector.class", AivenKafkaConnectS3SinkConnector.class.getName());
        config.put("aws.access.key.id", S3_ACCESS_KEY_ID);
        config.put("aws.secret.access.key", S3_SECRET_ACCESS_KEY);
        config.put("aws.s3.endpoint", s3Endpoint);
        config.put("aws.s3.bucket.name", TEST_BUCKET_NAME);
        config.put("topics", TEST_TOPIC_0 + "," + TEST_TOPIC_1);
        return config;
    }

    // WARN: different from GCS
    private String getOldBlobName(final int partition, final int startOffset, final String compression) {
        final String result = String.format("%s%s-%d-%020d", s3Prefix, TEST_TOPIC_0, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

    private String getNewBlobName(final int partition, final int startOffset, final String compression) {
        final String result = String.format("%s-%d-%d", TEST_TOPIC_0, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

    protected String getKeyBlobName(final String key, final String compression) {
        final String result = String.format("%s%s", s3Prefix, key);
        return result + CompressionType.forName(compression).extension();
    }
}
