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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class AivenKafkaConnectS3Config {
    public static ConfigDef newConfigDef() {

        final ConfigDef configDef = new ConfigDef();

        configDef.define(
            AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID,
            Type.PASSWORD,
            Importance.HIGH,
            "AWS Access Key ID"
        );

        configDef.define(
            AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY,
            Type.PASSWORD,
            Importance.HIGH,
            "AWS Secret Access Key"
        );

        configDef.define(
            AivenKafkaConnectS3Constants.AWS_S3_BUCKET,
            Type.STRING,
            Importance.HIGH,
            "AWS S3 Bucket name"
        );

        configDef.define(
            AivenKafkaConnectS3Constants.AWS_S3_ENDPOINT,
            Type.STRING,
            null,
            Importance.LOW,
            "Explicit AWS S3 Endpoint Address, mainly for testing"
        );

        configDef.define(
            AivenKafkaConnectS3Constants.AWS_S3_REGION,
            Type.STRING,
            Importance.HIGH,
            "AWS S3 Region, e.g. us-east-1"
        );

        configDef.define(
            AivenKafkaConnectS3Constants.AWS_S3_PREFIX,
            Type.STRING,
            null,
            Importance.MEDIUM,
            "Prefix for stored objects, e.g. cluster-1/"
        );

        configDef.define(
            AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION,
            Type.STRING,
            AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION_TYPE_GZIP,
            Importance.MEDIUM,
            "Output compression. Valid values are: "
                + AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION_TYPE_GZIP + " and "
                + AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION_TYPE_NONE
        );

        configDef.define(
            AivenKafkaConnectS3Constants.OUTPUT_FIELDS,
            Type.STRING,
            AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_VALUE,
            Importance.MEDIUM,
            "Output fields. A comma separated list of one or more: "
                + AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_KEY + ", "
                + AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_OFFSET + ", "
                + AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_TIMESTAMP + ", "
                + AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_VALUE + ", "
                + AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_HEADERS
        );

        return configDef;
    }
}
