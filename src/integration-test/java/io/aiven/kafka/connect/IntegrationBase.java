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
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.github.dockerjava.api.model.Ulimit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

public interface IntegrationBase {

    static LocalStackContainer createS3Container() {
        return new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:2.0.2")
        ).withServices(LocalStackContainer.Service.S3);
    }

    static AmazonS3 createS3Client(final LocalStackContainer localStackContainer) {
        return AmazonS3ClientBuilder
            .standard()
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                    localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                    localStackContainer.getRegion()
                )
            )
            .withCredentials(
                new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(localStackContainer.getAccessKey(), localStackContainer.getSecretKey())
                )
            )
            .build();
    }

    default AdminClient newAdminClient(final KafkaContainer kafka) {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        return AdminClient.create(adminClientConfig);
    }

    default ConnectRunner newConnectRunner(final KafkaContainer kafka,
                                           final File pluginDir,
                                           final int offsetFlushIntervalMs) {
        return new ConnectRunner(pluginDir, kafka.getBootstrapServers(), offsetFlushIntervalMs);
    }


    static void extractConnectorPlugin(File pluginDir) throws IOException, InterruptedException {
        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s",
            distFile, pluginDir.toString());
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;
    }

    static File getPluginDir() throws IOException {
        final File testDir = Files.createTempDirectory("s3-connector-for-apache-kafka-test-").toFile();

        final File pluginDir = new File(testDir, "plugins/s3-connector-for-apache-kafka/");
        assert pluginDir.mkdirs();
        return pluginDir;
    }

    static KafkaContainer createKafkaContainer() {
        return new KafkaContainer("5.2.1")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .withNetwork(Network.newNetwork())
            .withExposedPorts(KafkaContainer.KAFKA_PORT, 9092)
            .withCreateContainerCmdModifier(cmd ->
                cmd.getHostConfig().withUlimits(List.of(new Ulimit("nofile", 30000L, 30000L)))
            );
    }

    static String topicName(final TestInfo testInfo) {
        return testInfo.getTestMethod().get().getName() + "-" + testInfo.getDisplayName().hashCode();
    }

    static void createTopics(final AdminClient adminClient, final List<String> topicNames)
        throws ExecutionException, InterruptedException {
        final var newTopics = topicNames.stream()
            .map(s -> new NewTopic(s, 4, (short) 1))
            .collect(Collectors.toList());
        adminClient.createTopics(newTopics).all().get();
    }

    static void waitForRunningContainer(final Container<?> kafka) {
        Awaitility.await().atMost(Duration.ofMinutes(1)).until(kafka::isRunning);
    }
}
