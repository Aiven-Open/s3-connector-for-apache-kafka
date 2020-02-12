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

public class AivenKafkaConnectS3Constants {
    public static final String AWS_ACCESS_KEY_ID = "aws_access_key_id";
    public static final String AWS_S3_BUCKET = "aws_s3_bucket";
    public static final String AWS_S3_ENDPOINT = "aws_s3_endpoint";
    public static final String AWS_S3_PREFIX = "aws_s3_prefix";
    public static final String AWS_S3_REGION = "aws_s3_region";
    public static final String AWS_SECRET_ACCESS_KEY = "aws_secret_access_key";

    public static final String OUTPUT_COMPRESSION = "output_compression";
    public static final String OUTPUT_COMPRESSION_TYPE_GZIP = "gzip";
    public static final String OUTPUT_COMPRESSION_TYPE_NONE = "none";

    public static final String OUTPUT_FIELDS = "output_fields";
    public static final String OUTPUT_FIELD_NAME_KEY = "key";
    public static final String OUTPUT_FIELD_NAME_OFFSET = "offset";
    public static final String OUTPUT_FIELD_NAME_TIMESTAMP = "timestamp";
    public static final String OUTPUT_FIELD_NAME_VALUE = "value";
}
