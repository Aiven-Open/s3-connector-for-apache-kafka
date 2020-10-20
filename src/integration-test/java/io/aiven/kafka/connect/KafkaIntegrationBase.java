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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.testcontainers.containers.KafkaContainer;


public interface KafkaIntegrationBase {

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

    default KafkaProducer<byte[], byte[]> newProducer(final KafkaContainer kafka) {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(producerProps);
    }

    default Future<RecordMetadata> sendMessageAsync(final KafkaProducer<byte[], byte[]> producer,
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

    default Map<String, String> basicConnectorConfig(final String connectorName) {
        final Map<String, String> config = new HashMap<>();
        config.put("name", connectorName);
        config.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        config.put("tasks.max", "1");
        return config;
    }

    static void extractConnectorPlugin(File pluginDir) throws IOException, InterruptedException {
        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s",
            distFile.toString(), pluginDir.toString());
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;
    }

    static File getPluginDir() throws IOException {
        final File testDir = Files.createTempDirectory("aiven-kafka-connect-s3-test-").toFile();

        final File pluginDir = new File(testDir, "plugins/aiven-kafka-connect-s3/");
        assert pluginDir.mkdirs();
        return pluginDir;
    }
}
