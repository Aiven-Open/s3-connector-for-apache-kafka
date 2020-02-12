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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class AivenKafkaConnectS3Config {
    public static ConfigDef newConfigDef() {
        final ConfigDef configDef = new ConfigDef();

        configDef.define(
            AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID,
            Type.STRING,
            Importance.HIGH,
            "AWS Access Key ID"
        );

        configDef.define(
            AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY,
            Type.STRING,
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
                + AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION_TYPE_GZIP
                + " and "
                + AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION_TYPE_NONE
        );

        configDef.define(
            AivenKafkaConnectS3Constants.OUTPUT_FIELDS,
            Type.STRING,
            AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_VALUE,
            Importance.MEDIUM,
            "Output fields. A comma separated list of one or more: "
                + AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_KEY
                + ", " + AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_OFFSET
                + ", " + AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_TIMESTAMP
                + ", " + AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_VALUE
        );

        return configDef;
    }
}
