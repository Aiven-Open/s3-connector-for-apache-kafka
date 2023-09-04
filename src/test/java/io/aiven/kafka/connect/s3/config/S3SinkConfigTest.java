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

package io.aiven.kafka.connect.s3.config;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.s3.OldFullKeyFormatters;
import io.aiven.kafka.connect.s3.S3OutputStream;

import com.amazonaws.regions.Regions;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_ACCESS_KEY_ID;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_BUCKET;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_ENDPOINT;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_PREFIX;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_REGION;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_REGION_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_SECRET_ACCESS_KEY;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.OUTPUT_COMPRESSION;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.OUTPUT_FIELDS;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.TIMESTAMP_SOURCE;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.TIMESTAMP_TIMEZONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3SinkConfigTest {

    @Test
    void correctFullConfig() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "AWS_ACCESS_KEY_ID");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "AWS_SECRET_ACCESS_KEY");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_ENDPOINT_CONFIG, "AWS_S3_ENDPOINT");
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "AWS_S3_PREFIX");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        props.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.name);
        props.put(
            S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG,
            Arrays.stream(OutputFieldType.values())
                .map(OutputFieldType::name)
                .map(String::toLowerCase)
                .collect(Collectors.joining(","))
        );
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, OutputFieldEncodingType.NONE.name);

        final var conf = new S3SinkConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertThat(awsCredentials.getAccessKeyId().value()).isEqualTo("AWS_ACCESS_KEY_ID");
        assertThat(awsCredentials.getSecretAccessKey().value()).isEqualTo("AWS_SECRET_ACCESS_KEY");
        assertThat(conf.getAwsS3BucketName()).isEqualTo("THE_BUCKET");
        assertThat(conf.getAwsS3Prefix()).isEqualTo("AWS_S3_PREFIX");
        assertThat(conf.getAwsS3EndPoint()).isEqualTo("AWS_S3_ENDPOINT");
        assertThat(conf.getAwsS3Region()).isEqualTo(Regions.US_EAST_1);
        assertThat(conf.getCompressionType()).isEqualTo(CompressionType.GZIP);
        assertThat(conf.getOutputFieldEncodingType()).isEqualTo(OutputFieldEncodingType.NONE);
        assertThat(conf.getOutputFields())
            .containsExactly(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE)
            );
        assertThat(conf.getFormatType()).isEqualTo(FormatType.forName("csv"));
        assertThat(conf.getAwsS3PartSize()).isEqualTo(S3OutputStream.DEFAULT_PART_SIZE);
        assertThat(conf.getKafkaRetryBackoffMs()).isNull();
        assertThat(conf.getS3RetryBackoffDelayMs()).isEqualTo(S3SinkConfig.AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT);
        assertThat(conf.getS3RetryBackoffMaxDelayMs()).isEqualTo(
            S3SinkConfig.AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT);
        assertThat(conf.getS3RetryBackoffMaxRetries()).isEqualTo(S3SinkConfig.S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT);
    }

    @Test
    void correctFullConfigForOldStyleConfigParameters() {
        final var props = new HashMap<String, String>();

        props.put(AWS_ACCESS_KEY_ID, "AWS_ACCESS_KEY_ID");
        props.put(AWS_SECRET_ACCESS_KEY, "AWS_SECRET_ACCESS_KEY");
        props.put(AWS_S3_BUCKET, "THE_BUCKET");
        props.put(AWS_S3_ENDPOINT, "AWS_S3_ENDPOINT");
        props.put(AWS_S3_PREFIX, "AWS_S3_PREFIX");
        props.put(AWS_S3_REGION, Regions.US_EAST_1.getName());

        props.put(OUTPUT_COMPRESSION, CompressionType.GZIP.name);
        props.put(
            OUTPUT_FIELDS,
            Arrays.stream(OutputFieldType.values())
                .map(OutputFieldType::name)
                .map(String::toLowerCase)
                .collect(Collectors.joining(","))
        );

        final var conf = new S3SinkConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertThat(awsCredentials.getAccessKeyId().value()).isEqualTo("AWS_ACCESS_KEY_ID");
        assertThat(awsCredentials.getSecretAccessKey().value()).isEqualTo("AWS_SECRET_ACCESS_KEY");
        assertThat(conf.getAwsS3BucketName()).isEqualTo("THE_BUCKET");
        assertThat(conf.getAwsS3Prefix()).isEqualTo("AWS_S3_PREFIX");
        assertThat(conf.getAwsS3EndPoint()).isEqualTo("AWS_S3_ENDPOINT");
        assertThat(conf.getAwsS3Region()).isEqualTo(Regions.US_EAST_1);
        assertThat(conf.getCompressionType()).isEqualTo(CompressionType.GZIP);
        assertThat(conf.getOutputFieldEncodingType()).isEqualTo(OutputFieldEncodingType.BASE64);
        assertThat(conf.getOutputFields())
            .containsExactlyInAnyOrderElementsOf(
                Arrays.asList(
                    new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.BASE64),
                    new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64),
                    new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                    new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                    new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE)
                ));
    }

    @Test
    void newConfigurationPropertiesHaveHigherPriorityOverOldOne() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "AWS_ACCESS_KEY_ID");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "AWS_SECRET_ACCESS_KEY");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_ENDPOINT_CONFIG, "AWS_S3_ENDPOINT");
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "AWS_S3_PREFIX");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        props.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.name);
        props.put(
            S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG,
            Arrays.stream(OutputFieldType.values())
                .map(OutputFieldType::name)
                .map(String::toLowerCase)
                .collect(Collectors.joining(","))
        );
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, OutputFieldEncodingType.NONE.name);

        props.put(AWS_ACCESS_KEY_ID, "AWS_ACCESS_KEY_ID_#1");
        props.put(AWS_SECRET_ACCESS_KEY, "AWS_SECRET_ACCESS_KEY_#1");
        props.put(AWS_S3_BUCKET, "THE_BUCKET_#1");
        props.put(AWS_S3_ENDPOINT, "AWS_S3_ENDPOINT_#1");
        props.put(AWS_S3_PREFIX, "AWS_S3_PREFIX_#1");
        props.put(AWS_S3_REGION, Regions.US_WEST_1.getName());

        props.put(OUTPUT_COMPRESSION, CompressionType.NONE.name);
        props.put(OUTPUT_FIELDS, "key, value");

        final var conf = new S3SinkConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertThat(awsCredentials.getAccessKeyId().value()).isEqualTo("AWS_ACCESS_KEY_ID");
        assertThat(awsCredentials.getSecretAccessKey().value()).isEqualTo("AWS_SECRET_ACCESS_KEY");
        assertThat(conf.getAwsS3BucketName()).isEqualTo("THE_BUCKET");
        assertThat(conf.getAwsS3Prefix()).isEqualTo("AWS_S3_PREFIX");
        assertThat(conf.getAwsS3EndPoint()).isEqualTo("AWS_S3_ENDPOINT");
        assertThat(conf.getAwsS3Region()).isEqualTo(Regions.US_EAST_1);
        assertThat(conf.getCompressionType()).isEqualTo(CompressionType.GZIP);
        assertThat(conf.getOutputFieldEncodingType()).isEqualTo(OutputFieldEncodingType.NONE);
        assertThat(conf.getOutputFields()).containsExactlyInAnyOrderElementsOf(
            Arrays.asList(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE)
            )
        );
    }

    @Test
    final void emptyAwsAccessKeyID() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [hidden] for configuration aws_access_key_id: Password must be non-empty");

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [hidden] for configuration aws.access.key.id: Password must be non-empty");
    }

    @Test
    final void emptyAwsSecretAccessKey() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [hidden] for configuration aws_secret_access_key: Password must be non-empty");

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [hidden] for configuration aws.secret.access.key: Password must be non-empty");
    }

    @Test
    void wrongPartSize() {
        final var wrongMaxPartSizeProps =
            Map.of(
                S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-key-id",
                S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "bla-bla-access-key",
                S3SinkConfig.AWS_S3_PART_SIZE, Long.toString(2_000_000_001L)
            );
        assertThatThrownBy(() -> new S3SinkConfig(wrongMaxPartSizeProps))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 2000000001 for configuration aws.s3.part.size.bytes: "
                + "Part size must be no more: 2000000000 bytes (2GB)");

        final var wrongMinPartSizeProps =
            Map.of(
                S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-key-id",
                S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "bla-bla-access-key",
                S3SinkConfig.AWS_S3_PART_SIZE, "0"
            );
        assertThatThrownBy(() -> new S3SinkConfig(wrongMinPartSizeProps))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration aws.s3.part.size.bytes: Part size must be greater than 0");
    }

    @Test
    final void emptyAwsS3Bucket() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(AWS_S3_BUCKET, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value  for configuration aws_s3_bucket: String must be non-empty");

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value  for configuration aws.s3.bucket.name: String must be non-empty");
    }

    @Test
    final void emptyAwsS3Region() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(AWS_S3_BUCKET, "blah-blah-blah");
        props.put(AWS_S3_REGION, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value  for configuration aws_s3_region: "
                + "supported values are: "
                + Arrays.stream(Regions.values()).map(Regions::getName).collect(Collectors.joining(", ")));

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value  for configuration aws.s3.region: "
                    + "supported values are: "
                    + Arrays.stream(Regions.values()).map(Regions::getName).collect(Collectors.joining(", ")));
    }

    @Test
    final void emptyAwsS3Prefix() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(AWS_S3_BUCKET, "blah-blah-blah");
        props.put(AWS_S3_REGION, Regions.US_WEST_1.getName());
        props.put(AWS_S3_PREFIX, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value  for configuration aws_s3_prefix: String must be non-empty"
            );

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value  for configuration aws.s3.prefix: String must be non-empty"
            );
    }

    @Test
    final void emptyAwsS3EndPoint() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(AWS_S3_BUCKET, "blah-blah-blah");
        props.put(AWS_S3_REGION, Regions.US_EAST_1.getName());
        props.put(AWS_S3_PREFIX, "blah-blah-blah");
        props.put(AWS_S3_ENDPOINT, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value  for configuration aws_s3_endpoint: String must be non-empty"
            );

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_ENDPOINT_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value  for configuration aws.s3.endpoint: String must be non-empty"
            );
    }

    @Test
    final void wrongAwsS3EndPoint() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(AWS_S3_BUCKET, "blah-blah-blah");
        props.put(AWS_S3_REGION, Regions.US_EAST_1.getName());
        props.put(AWS_S3_PREFIX, "blah-blah-blah");
        props.put(AWS_S3_ENDPOINT, "ffff://asdsadas");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value ffff://asdsadas for configuration aws_s3_endpoint: should be valid URL"
            );

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_ENDPOINT_CONFIG, "ffff://asdsadas");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value ffff://asdsadas for configuration aws.s3.endpoint: should be valid URL"
            );
    }


    @Test
    void emptyOutputField() {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(OUTPUT_FIELDS, "");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [] for configuration output_fields: cannot be empty");

        props.remove(OUTPUT_FIELDS);
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [] for configuration format.output.fields: cannot be empty");
    }

    @Test
    void supportPriorityForOutputFields() {
        final var props = Maps.<String, String>newHashMap();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");

        props.put(OUTPUT_FIELDS, "key,value,offset,timestamp");
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key");

        final var conf = new S3SinkConfig(props);

        assertThat(conf.getOutputFields())
            .containsExactly(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.BASE64));
    }

    @Test
    void unsupportedOutputField() {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(OUTPUT_FIELDS, "key,value,offset,timestamp,unsupported");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value [key, value, offset, timestamp, unsupported] "
                    + "for configuration output_fields: "
                    + "supported values are: 'key', 'value', 'offset', 'timestamp', 'headers'"
            );

        props.remove(OUTPUT_FIELDS);
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,offset,timestamp,unsupported");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [key, value, offset, timestamp, unsupported] "
                + "for configuration format.output.fields: "
                + "supported values are: 'key', 'value', 'offset', 'timestamp', 'headers'"
            );
    }

    @Test
    void customAwsS3BackoffPolicy() {
        final var props = Map.of(
            S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG, "2000",
            S3SinkConfig.AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, "4000",
            S3SinkConfig.AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG, "10"
        );
        final var config = new S3SinkConfig(props);

        assertThat(config.getS3RetryBackoffDelayMs()).isEqualTo(2000L);
        assertThat(config.getS3RetryBackoffMaxDelayMs()).isEqualTo(4000L);
        assertThat(config.getS3RetryBackoffMaxRetries()).isEqualTo(10);
    }

    @Test
    void wrongAwsS3BackoffPolicy() {
        final var wrongDelayProps = Map.of(
            S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG, "0"
        );
        assertThatThrownBy(() -> new S3SinkConfig(wrongDelayProps))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration aws.s3.backoff.delay.ms: Value must be at least 1");

        final var wrongMaxDelayProps = Map.of(
            S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG, "0"
        );
        assertThatThrownBy(() -> new S3SinkConfig(wrongMaxDelayProps))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration aws.s3.backoff.max.delay.ms: Value must be at least 1");

        final var wrongMaxRetriesProps = Map.of(
            S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG, "0"
        );
        assertThatThrownBy(() -> new S3SinkConfig(wrongMaxRetriesProps))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration aws.s3.backoff.max.retries: Value must be at least 1");

        final var tooBigMaxRetriesProps = Map.of(
            S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah",
            S3SinkConfig.AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG, "35"
        );
        assertThatThrownBy(() -> new S3SinkConfig(tooBigMaxRetriesProps))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 35 for configuration aws.s3.backoff.max.retries: "
                + "Value must be no more than 30");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    void supportedCompression(final String compression) {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        if (!Objects.isNull(compression)) {
            props.put(OUTPUT_COMPRESSION, compression);
        }

        var config = new S3SinkConfig(props);
        assertThat(config.getCompressionType()).isEqualTo(determineExpectedCompressionType(compression));

        props.remove(OUTPUT_COMPRESSION);
        if (!Objects.isNull(compression)) {
            props.put(FILE_COMPRESSION_TYPE_CONFIG, compression);
        }

        config = new S3SinkConfig(props);
        assertThat(config.getCompressionType())
            .isEqualTo(determineExpectedCompressionType(compression));
    }

    @Test
    void supportPriorityForCompressionTypeConfig() {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(OUTPUT_COMPRESSION, CompressionType.GZIP.name);
        props.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.NONE.name);

        final var config = new S3SinkConfig(props);

        assertThat(config.getCompressionType()).isEqualTo(CompressionType.NONE);
    }

    private CompressionType determineExpectedCompressionType(final String compression) {
        if (Objects.isNull(compression) || "gzip".equals(compression)) {
            return CompressionType.GZIP;
        } else if ("none".equals(compression)) {
            return CompressionType.NONE;
        } else if ("snappy".equals(compression)) {
            return CompressionType.SNAPPY;
        } else if ("zstd".equals(compression)) {
            return CompressionType.ZSTD;
        } else {
            throw new RuntimeException("Shouldn't be here");
        }
    }

    @Test
    void unsupportedCompressionType() {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,offset,timestamp");
        props.put(OUTPUT_COMPRESSION, "unsupported");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value unsupported for configuration output_compression: "
                    + "supported values are: 'none', 'gzip', 'snappy', 'zstd'");

        props.remove(OUTPUT_COMPRESSION);
        props.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, "unsupported");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value unsupported for configuration file.compression.type: "
                + "supported values are: 'none', 'gzip', 'snappy', 'zstd'");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldBuildPrefixTemplate(final boolean yyyyUppercase) {
        final String prefix;
        if (yyyyUppercase) {
            prefix = "{{timestamp:unit=YYYY}}/{{timestamp:unit=MM}}/{{timestamp:unit=dd}}/";
        } else {
            prefix = "{{timestamp:unit=yyyy}}/{{timestamp:unit=MM}}/{{timestamp:unit=dd}}/";
        }

        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,offset,timestamp,headers");
        props.put(TIMESTAMP_TIMEZONE, "Europe/Berlin");
        props.put(TIMESTAMP_SOURCE, "wallclock");
        props.put(AWS_S3_PREFIX_CONFIG, prefix);

        final var config = new S3SinkConfig(props);

        // null record is fine here, because it's not needed for the wallclock timestamp source
        final var expectedTimestamp = config.getTimestampSource().time(null);

        final var renderedPrefix =
            config.getPrefixTemplate()
                .instance()
                .bindVariable("timestamp", parameter ->
                    // null record is fine here, because it's not needed for the wall clock timestamp source
                    OldFullKeyFormatters.timestamp(null, config.getTimestampSource(), parameter))
                .render();

        assertThat(renderedPrefix)
            .isEqualTo(
                String.format(
                    "%s/%s/%s/",
                    expectedTimestamp.format(DateTimeFormatter.ofPattern("yyyy")),
                    expectedTimestamp.format(DateTimeFormatter.ofPattern("MM")),
                    expectedTimestamp.format(DateTimeFormatter.ofPattern("dd"))
                ));
    }

    @ParameterizedTest
    @ValueSource(strings = {"jsonl", "json", "csv"})
    void supportedFormatTypeConfig(final String formatType) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "any_access_key_id");
        properties.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "any_secret_key");
        properties.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "any_bucket");
        properties.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "any_prefix");
        properties.put(S3SinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, formatType);

        final S3SinkConfig c = new S3SinkConfig(properties);
        final FormatType expectedFormatType = FormatType.forName(formatType);

        assertThat(c.getFormatType()).isEqualTo(expectedFormatType);
    }

    @Test
    void wrongFormatTypeConfig() {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "unknown");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value unknown for configuration format.output.type: "
                    + "supported values are: 'avro', 'csv', 'json', 'jsonl', 'parquet'");

    }

    @Test
    void notSupportYyyyUppercaseInFilenameTemplate() {
        final Map<String, String> properties =
            Map.of(
                S3SinkConfig.FILE_NAME_TEMPLATE_CONFIG,
                "{{topic}}-"
                    + "{{timestamp:unit=YYYY}}"
                    + "-{{partition}}-{{start_offset:padding=true}}.gz",
                S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "any_access_key_id",
                S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "any_secret_key",
                S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "any_bucket"
            );
        assertThatThrownBy(() -> new S3SinkConfig(properties))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value {{topic}}-{{timestamp:unit=YYYY}}-{{partition}}-{{start_offset:padding=true}}.gz "
                    + "for configuration file.name.template: unsupported set of template variables parameters, "
                    + "supported sets are: "
                    + "partition:padding=true|false,start_offset:padding=true|false,timestamp:unit=yyyy|MM|dd|HH");
    }

    @Test
    void stsRoleCorrectConfig() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_EXTERNAL_ID, "EXTERNAL_ID");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        final var conf = new S3SinkConfig(props);

        assertThat(conf.getStsRole().getArn()).isEqualTo("arn:aws:iam::12345678910:role/S3SinkTask");
        assertThat(conf.getStsRole().getExternalId()).isEqualTo("EXTERNAL_ID");
        assertThat(conf.getStsRole().getSessionName()).isEqualTo("SESSION_NAME");
        assertThat(conf.getAwsS3Region()).isEqualTo(Regions.US_EAST_1);
    }

    @Test
    void stsRoleEmptyArn() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_EXTERNAL_ID, "EXTERNAL_ID");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Either {aws.access.key.id, aws.secret.access.key} or"
                    + " {aws.sts.role.arn, aws.sts.role.session.name} should be set");
    }

    @Test
    void stsRoleEmptySessionName() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_EXTERNAL_ID, "EXTERNAL_ID");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Either {aws.access.key.id, aws.secret.access.key} or"
                    + " {aws.sts.role.arn, aws.sts.role.session.name} should be set"
            );
    }

    @Test
    void stsWrongSessionDuration() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_DURATION, "30");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 30 for configuration aws.sts.role.session.duration: Value must be at least 900");

        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_DURATION, "50000");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage(
                "Invalid value 50000 for configuration aws.sts.role.session.duration: "
                    + "Value must be no more than 43200");
    }

    @Test
    void stsCorrectSessionDuration() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_DURATION, "900");

        final var conf = new S3SinkConfig(props);

        assertThat(conf.getStsRole().getSessionDurationSeconds()).isEqualTo(900);
    }

    @Test
    void stsEndpointShouldNotBeSetWithoutRegion() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_EXTERNAL_ID, "EXTERNAL_ID");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_STS_CONFIG_ENDPOINT, "https://sts.eu-north-1.amazonaws.com");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.s3.region should be specified together with aws.sts.config.endpoint");
    }

    @ParameterizedTest
    @ValueSource(strings = {"{{key}}", "{{topic}}/{{partition}}/{{key}}"})
    void notSupportedFileMaxRecords(final String fileNameTemplate) {
        final Map<String, String> properties =
            Map.of(
                S3SinkConfig.FILE_NAME_TEMPLATE_CONFIG, fileNameTemplate,
                S3SinkConfig.FILE_MAX_RECORDS, "2",
                S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "any_access_key_id",
                S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "any_secret_key",
                S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "any_bucket"
            );
        assertThatThrownBy(() -> new S3SinkConfig(properties))
            .isInstanceOf(ConfigException.class)
            .hasMessage(String.format(
                "When file.name.template is %s, file.max.records must be either 1 or not set",
                fileNameTemplate
            ));
    }

}
