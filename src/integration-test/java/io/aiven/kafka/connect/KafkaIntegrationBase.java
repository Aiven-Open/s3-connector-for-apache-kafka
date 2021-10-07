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
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

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


    static void extractConnectorPlugin(File pluginDir) throws IOException, InterruptedException {
        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();

        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s",
            distFile.toString(), pluginDir.toString());
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;
    }

    static File getPluginDir() throws IOException {
        final File testDir = Files.createTempDirectory("s3-connector-for-apache-kafka-test-").toFile();

        final File pluginDir = new File(testDir, "plugins/s3-connector-for-apache-kafka/");
        assert pluginDir.mkdirs();
        return pluginDir;
    }
}
