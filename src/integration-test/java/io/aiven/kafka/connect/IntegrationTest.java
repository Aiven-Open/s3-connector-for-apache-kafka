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

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

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
        testBucketAccessor.createBucket();

        pluginDir = KafkaIntegrationBase.getPluginDir();
        KafkaIntegrationBase.extractConnectorPlugin(pluginDir);
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        adminClient = newAdminClient(kafka);
        producer = newProducer(kafka);

        final NewTopic newTopic0 = new NewTopic(TEST_TOPIC_0, 4, (short) 1);
        adminClient.createTopics(Arrays.asList(newTopic0)).all().get();

        connectRunner = newConnectRunner(kafka, pluginDir, OFFSET_FLUSH_INTERVAL_MS);
        connectRunner.start();
    }

    @AfterEach
    final void tearDown() {
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
        connectRunner.createConnector(connectorConfig);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "value-" + cnt;
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
            getBlobName(0, 0, compression),
            getBlobName(1, 0, compression),
            getBlobName(2, 0, compression),
            getBlobName(3, 0, compression));
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

        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final String key = "key-" + cnt;
                final String value = "value-" + cnt;
                cnt += 1;

                final String blobName = getBlobName(partition, 0, compression);
                final String actualLine = blobContents.get(blobName).get(i);
                final String expectedLine = key + "," + value;
                assertEquals(expectedLine, actualLine);
            }
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
            getBlobName(0, 0, compression),
            getBlobName(1, 0, compression),
            getBlobName(2, 0, compression),
            getBlobName(3, 0, compression));
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

                final String blobName = getBlobName(partition, 0, "none");
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
        config.put("aws.s3.prefix", s3Prefix);
        config.put("topics", TEST_TOPIC_0);
        return config;
    }

    // WARN: different from GCS
    private String getBlobName(final int partition, final int startOffset, final String compression) {
        //
        final String result = String.format("%s%s-%d-%020d", s3Prefix, TEST_TOPIC_0, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }
}
