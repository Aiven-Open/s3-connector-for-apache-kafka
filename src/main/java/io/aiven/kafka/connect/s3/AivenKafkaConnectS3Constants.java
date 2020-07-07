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

import java.util.HashSet;
import java.util.Set;

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
    public static final String OUTPUT_FIELD_NAME_HEADERS = "headers";

    public static final Set<String> OUTPUT_FILED_NAMES = new HashSet<String>() {
        {
            add(OUTPUT_FIELD_NAME_KEY);
            add(OUTPUT_FIELD_NAME_OFFSET);
            add(OUTPUT_FIELD_NAME_TIMESTAMP);
            add(OUTPUT_FIELD_NAME_VALUE);
            add(OUTPUT_FIELD_NAME_HEADERS);
        }
    };
}
