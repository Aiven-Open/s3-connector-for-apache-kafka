/*
 * Copyright 2020 Aiven Oy
 * Copyright 2018 Confluent Inc.
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

package io.aiven.kafka.connect.s3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AivenKafkaConnectS3SinkConnector extends Connector {

    private static final Logger LOGGER = LoggerFactory.getLogger(AivenKafkaConnectS3SinkConnector.class);

    private Map<String, String> configProperties;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AivenKafkaConnectS3SinkTask.class;
    }

    @Override
    public void start(final Map<String, String> properties) {
        final String[] mandatoryKeys = new String[] {
            AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID,
            AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY,
            AivenKafkaConnectS3Constants.AWS_S3_BUCKET
        };
        for (final String propertyKey: mandatoryKeys) {
            if (properties.get(propertyKey) == null) {
                throw new ConnectException("Mandatory parameter '" + propertyKey + "' is missing.");
            }
        }
        final String fieldConfig = properties.get(AivenKafkaConnectS3Constants.OUTPUT_FIELDS);
        if (fieldConfig != null) {
            final String[] fieldNames = fieldConfig.split("\\s*,\\s*");
            for (int i = 0; i < fieldNames.length; i++) {
                if (fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_KEY)
                    || fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_OFFSET)
                    || fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_TIMESTAMP)
                    || fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_VALUE)) {
                    // pass
                } else {
                    throw new ConnectException("Unknown output field name '" + fieldNames[i] + "'.");
                }
            }
        }
        configProperties = properties;
    }

    @Override
    public void stop() {
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final List<Map<String, String>> taskConfigs = new ArrayList<>();
        final Map<String, String> taskProperties = new HashMap<>();
        taskProperties.putAll(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProperties);
        }
        return taskConfigs;
    }

    @Override
    public ConfigDef config() {
        return AivenKafkaConnectS3Config.newConfigDef();
    }
}
