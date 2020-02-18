/*
 * Copyright (C) 2020 Aiven Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

public class AivenKafkaConnectS3SinkConnector extends Connector {

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
        for (final String pk: mandatoryKeys) {
            if (properties.get(pk) == null) {
                throw new ConnectException("Mandatory parameter '" + pk + "' is missing.");
            }
        }
        final String fieldConfig = properties.get(AivenKafkaConnectS3Constants.OUTPUT_FIELDS);
        if (fieldConfig != null) {
            final String[] fieldNames = fieldConfig.split("\\s*,\\s*");
            for (int i = 0; i < fieldNames.length; i++) {
                //FIXME simplify if/else statements
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
