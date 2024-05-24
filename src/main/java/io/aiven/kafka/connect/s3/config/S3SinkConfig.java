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

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FixedSetRecommender;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import io.aiven.kafka.connect.common.config.validators.FilenameTemplateValidator;
import io.aiven.kafka.connect.common.config.validators.NonEmptyPassword;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsValidator;
import io.aiven.kafka.connect.common.config.validators.TimeZoneValidator;
import io.aiven.kafka.connect.common.config.validators.TimestampSourceValidator;
import io.aiven.kafka.connect.common.config.validators.UrlValidator;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.s3.S3OutputStream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.internal.BucketNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3SinkConfig extends AivenCommonConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3SinkConfig.class);

    @Deprecated
    public static final String AWS_ACCESS_KEY_ID = "aws_access_key_id";
    @Deprecated
    public static final String AWS_SECRET_ACCESS_KEY = "aws_secret_access_key";
    @Deprecated
    public static final String AWS_S3_BUCKET = "aws_s3_bucket";
    @Deprecated
    public static final String AWS_S3_ENDPOINT = "aws_s3_endpoint";
    @Deprecated
    public static final String AWS_S3_REGION = "aws_s3_region";
    @Deprecated
    public static final String AWS_S3_PREFIX = "aws_s3_prefix";

    @Deprecated
    public static final String OUTPUT_COMPRESSION = "output_compression";
    @Deprecated
    public static final String OUTPUT_COMPRESSION_TYPE_GZIP = "gzip";
    @Deprecated
    public static final String OUTPUT_COMPRESSION_TYPE_NONE = "none";

    @Deprecated
    public static final String OUTPUT_FIELDS = "output_fields";
    @Deprecated
    public static final String TIMESTAMP_TIMEZONE = "timestamp.timezone";
    @Deprecated
    public static final String TIMESTAMP_SOURCE = "timestamp.source";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_KEY = "key";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_OFFSET = "offset";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_TIMESTAMP = "timestamp";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_VALUE = "value";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_HEADERS = "headers";
    @Deprecated
    public static final String AWS_S3_PREFIX_CONFIG = "aws.s3.prefix";

    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    public static final String AWS_CREDENTIALS_PROVIDER_CONFIG = "aws.credentials.provider";
    public static final String AWS_CREDENTIAL_PROVIDER_DEFAULT =
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";
    public static final String AWS_S3_BUCKET_NAME_CONFIG = "aws.s3.bucket.name";
    public static final String AWS_S3_SSE_ALGORITHM_CONFIG = "aws.s3.sse.algorithm";
    public static final String AWS_S3_ENDPOINT_CONFIG = "aws.s3.endpoint";
    public static final String AWS_S3_REGION_CONFIG = "aws.s3.region";
    public static final String AWS_S3_PART_SIZE = "aws.s3.part.size.bytes";

    // FIXME since we support so far both old style and new style of property names
    //      Importance was set to medium,
    //      as soon we will migrate to new values it must be set to HIGH
    //      same for default value

    public static final String AWS_STS_ROLE_ARN = "aws.sts.role.arn";
    public static final String AWS_STS_ROLE_EXTERNAL_ID = "aws.sts.role.external.id";
    public static final String AWS_STS_ROLE_SESSION_NAME = "aws.sts.role.session.name";
    public static final String AWS_STS_ROLE_SESSION_DURATION = "aws.sts.role.session.duration";
    public static final String AWS_STS_CONFIG_ENDPOINT = "aws.sts.config.endpoint";

    private static final String GROUP_AWS = "AWS";
    private static final String GROUP_FILE = "File";
    private static final String GROUP_FORMAT = "Format";
    private static final String GROUP_AWS_STS = "AWS STS";

    private static final String GROUP_S3_RETRY_BACKOFF_POLICY = "S3 retry backoff policy";

    public static final String AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG = "aws.s3.backoff.delay.ms";
    public static final String AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "aws.s3.backoff.max.delay.ms";
    public static final String AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG = "aws.s3.backoff.max.retries";
    // Default values from AWS SDK, since they are hidden
    public static final int AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT = 100;
    public static final int AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 20_000;
    // Comment in AWS SDK for max retries:
    // Maximum retry limit. Avoids integer overflow issues.
    //
    // NOTE: If the value is greater than 30, there can be integer overflow
    // issues during delay calculation.
    //in other words we can't use values greater than 30
    public static final int S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT = 3;

    public S3SinkConfig(final Map<String, String> properties) {
        super(configDef(), preprocessProperties(properties));
        validate();
    }

    static Map<String, String> preprocessProperties(final Map<String, String> properties) {
        // Add other preprocessings when needed here. Mind the order.
        return handleDeprecatedYyyyUppercase(properties);
    }

    private static Map<String, String> handleDeprecatedYyyyUppercase(final Map<String, String> properties) {
        if (!properties.containsKey(AWS_S3_PREFIX_CONFIG) && !properties.containsKey(AWS_S3_PREFIX)) {
            return properties;
        }

        final var result = new HashMap<>(properties);
        for (final var prop : List.of(AWS_S3_PREFIX_CONFIG, AWS_S3_PREFIX)) {
            if (properties.containsKey(prop)) {
                String template = properties.get(prop);
                final String originalTemplate = template;

                final var unitYyyyPattern = Pattern.compile("\\{\\{\\s*timestamp\\s*:\\s*unit\\s*=\\s*YYYY\\s*}}");
                template = unitYyyyPattern.matcher(template)
                    .replaceAll(matchResult -> matchResult.group().replace("YYYY", "yyyy"));

                if (!template.equals(originalTemplate)) {
                    LOGGER.warn("{{timestamp:unit=YYYY}} is no longer supported, "
                            + "please use {{timestamp:unit=yyyy}} instead. "
                            + "It was automatically replaced: {}",
                        template);
                }

                result.put(prop, template);
            }
        }
        return result;
    }

    public static ConfigDef configDef() {
        final var configDef = new S3SinkConfigDef();
        addAwsConfigGroup(configDef);
        addAwsStsConfigGroup(configDef);
        addFileConfigGroup(configDef);
        addOutputFieldsFormatConfigGroup(configDef, null);
        addDeprecatedTimestampConfig(configDef);
        addDeprecatedConfiguration(configDef);
        addKafkaBackoffPolicy(configDef);
        addS3RetryPolicies(configDef);
        return configDef;
    }

    private static void addAwsConfigGroup(final ConfigDef configDef) {
        int awsGroupCounter = 0;

        configDef.define(
            AWS_ACCESS_KEY_ID_CONFIG,
            Type.PASSWORD,
            null,
            new NonEmptyPassword(),
            Importance.MEDIUM,
            "AWS Access Key ID",
            GROUP_AWS,
            awsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_ACCESS_KEY_ID_CONFIG
        );

        configDef.define(
            AWS_SECRET_ACCESS_KEY_CONFIG,
            Type.PASSWORD,
            null,
            new NonEmptyPassword(),
            Importance.MEDIUM,
            "AWS Secret Access Key",
            GROUP_AWS,
            awsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_SECRET_ACCESS_KEY_CONFIG
        );

        configDef.define(
                AWS_CREDENTIALS_PROVIDER_CONFIG,
                Type.CLASS,
                AWS_CREDENTIAL_PROVIDER_DEFAULT,
                Importance.MEDIUM,
                "When you initialize a new "
                        + "service client without supplying any arguments, "
                        + "the AWS SDK for Java attempts to find temporary "
                        + "credentials by using the default credential "
                        + "provider chain implemented by the "
                        + "DefaultAWSCredentialsProviderChain class.",

                GROUP_AWS,
                awsGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_CREDENTIALS_PROVIDER_CONFIG
        );

        configDef.define(
            AWS_S3_BUCKET_NAME_CONFIG,
            Type.STRING,
            null,
            new BucketNameValidator(),
            Importance.MEDIUM,
            "AWS S3 Bucket name",
            GROUP_AWS,
            awsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_S3_BUCKET_NAME_CONFIG
        );

        // AWS S3 Server Side Encryption Algorithm configuration
        // Example values: 'AES256' for S3-managed keys, 'aws:kms' for AWS KMS-managed keys
        configDef.define(
            AWS_S3_SSE_ALGORITHM_CONFIG,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            Importance.MEDIUM,
            "AWS S3 Server Side Encryption Algorithm. Example values: 'AES256', 'aws:kms'.",
            GROUP_AWS,
            awsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_S3_SSE_ALGORITHM_CONFIG
        );

        configDef.define(
            AWS_S3_ENDPOINT_CONFIG,
            Type.STRING,
            null,
            new UrlValidator(),
            Importance.LOW,
            "Explicit AWS S3 Endpoint Address, mainly for testing",
            GROUP_AWS,
            awsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_S3_ENDPOINT_CONFIG
        );

        configDef.define(
            AWS_S3_REGION_CONFIG,
            Type.STRING,
            null,
            new AwsRegionValidator(),
            Importance.MEDIUM,
            "AWS S3 Region, e.g. us-east-1",
            GROUP_AWS,
            awsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_S3_REGION_CONFIG
        );

        configDef.define(
                AWS_S3_PART_SIZE,
                Type.INT,
                S3OutputStream.DEFAULT_PART_SIZE,
                new ConfigDef.Validator() {

                    static final int MAX_BUFFER_SIZE = 2_000_000_000;

                    @Override
                    public void ensureValid(final String name, final Object value) {
                        if (value == null) {
                            throw new ConfigException(name, null, "Part size must be non-null");
                        }
                        final var number = (Number) value;
                        if (number.longValue() <= 0) {
                            throw new ConfigException(
                                    name,
                                    value,
                                    "Part size must be greater than 0"
                            );
                        }
                        if (number.longValue() > MAX_BUFFER_SIZE) {
                            throw new ConfigException(
                                    name,
                                    value,
                                    "Part size must be no more: " + MAX_BUFFER_SIZE + " bytes (2GB)"
                            );
                        }
                    }
                },
                Importance.MEDIUM,
                "The Part Size in S3 Multi-part Uploads in bytes. Maximum is "
                        + Integer.MAX_VALUE + " (2GB) and default is "
                        + S3OutputStream.DEFAULT_PART_SIZE + " (5MB)",
                GROUP_AWS,
                awsGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_S3_PART_SIZE
        );

    }

    private static class BucketNameValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            try {
                if (value != null) {
                    BucketNameUtils.validateBucketName((String) value);
                }
            } catch (final IllegalArgumentException e) {
                throw new ConfigException("Illegal bucket name: " + e.getMessage());
            }
        }
    }

    private static void addS3RetryPolicies(final ConfigDef configDef) {
        var retryPolicyGroupCounter = 0;
        configDef.define(
                AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG,
                ConfigDef.Type.LONG,
                AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT,
                ConfigDef.Range.atLeast(1L),
                ConfigDef.Importance.MEDIUM,
                "S3 default base sleep time for non-throttled exceptions in milliseconds. "
                        + "Default is " + AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT + ".",
                GROUP_S3_RETRY_BACKOFF_POLICY,
                retryPolicyGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG
        );
        configDef.define(
                AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG,
                ConfigDef.Type.LONG,
                AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT,
                ConfigDef.Range.atLeast(1L),
                ConfigDef.Importance.MEDIUM,
                "S3 maximum back-off time before retrying a request in milliseconds. "
                        + "Default is " + AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT + ".",
                GROUP_S3_RETRY_BACKOFF_POLICY,
                retryPolicyGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG
        );
        configDef.define(
                AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG,
                ConfigDef.Type.INT,
                S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT,
                ConfigDef.Range.between(1L, 30),
                ConfigDef.Importance.MEDIUM,
                "Maximum retry limit "
                        + "(if the value is greater than 30, "
                        + "there can be integer overflow issues during delay calculation). "
                        + "Default is " + S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT + ".",
                GROUP_S3_RETRY_BACKOFF_POLICY,
                retryPolicyGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG
        );
    }

    private static void addAwsStsConfigGroup(final ConfigDef configDef) {
        int awsStsGroupCounter = 0;
        configDef.define(
            AWS_STS_ROLE_ARN,
            ConfigDef.Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            Importance.MEDIUM,
            "AWS STS Role",
            GROUP_AWS_STS,
            awsStsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_STS_ROLE_ARN
        );

        configDef.define(
            AWS_STS_ROLE_SESSION_NAME,
            ConfigDef.Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            Importance.MEDIUM,
            "AWS STS Session name",
            GROUP_AWS_STS,
            awsStsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_STS_ROLE_SESSION_NAME
        );

        configDef.define(
            AWS_STS_ROLE_SESSION_DURATION,
            ConfigDef.Type.INT,
            3600,
            ConfigDef.Range.between(AwsStsRole.MIN_SESSION_DURATION, AwsStsRole.MAX_SESSION_DURATION),
            Importance.MEDIUM,
            "AWS STS Session duration",
            GROUP_AWS_STS,
            awsStsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_STS_ROLE_SESSION_DURATION
        );

        configDef.define(
            AWS_STS_ROLE_EXTERNAL_ID,
            ConfigDef.Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            Importance.MEDIUM,
            "AWS STS External Id",
            GROUP_AWS_STS,
            awsStsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_STS_ROLE_EXTERNAL_ID
        );

        configDef.define(
            AWS_STS_CONFIG_ENDPOINT,
            ConfigDef.Type.STRING,
            AwsStsEndpointConfig.AWS_STS_GLOBAL_ENDPOINT,
            new ConfigDef.NonEmptyString(),
            Importance.MEDIUM,
            "AWS STS Config Endpoint",
            GROUP_AWS_STS,
            awsStsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_STS_CONFIG_ENDPOINT
        );
    }

    private static void addFileConfigGroup(final ConfigDef configDef) {
        int fileGroupCounter = 0;

        configDef.define(
            FILE_NAME_TEMPLATE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            new FilenameTemplateValidator(FILE_NAME_TEMPLATE_CONFIG),
            ConfigDef.Importance.MEDIUM,
            "The template for file names on S3. "
                + "Supports `{{ variable }}` placeholders for substituting variables. "
                + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                + "(the offset of the first record in the file). "
                + "Only some combinations of variables are valid, which currently are:\n"
                + "- `topic`, `partition`, `start_offset`."
                + "There is also `key` only variable {{key}} for grouping by keys",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.LONG,
            FILE_NAME_TEMPLATE_CONFIG
        );

        final String supportedCompressionTypes = CompressionType.names().stream()
            .map(f -> "'" + f + "'")
            .collect(Collectors.joining(", "));

        configDef.define(
            FILE_COMPRESSION_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            new FileCompressionTypeValidator(),
            ConfigDef.Importance.MEDIUM,
            "The compression type used for files put on S3. "
                + "The supported values are: " + supportedCompressionTypes + ".",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.NONE,
            FILE_COMPRESSION_TYPE_CONFIG,
            FixedSetRecommender.ofSupportedValues(CompressionType.names())
        );

        configDef.define(
            FILE_MAX_RECORDS,
            ConfigDef.Type.INT,
            0,
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    assert value instanceof Integer;
                    if ((Integer) value < 0) {
                        throw new ConfigException(
                            FILE_MAX_RECORDS, value,
                            "must be a non-negative integer number");
                    }
                }
            },
            ConfigDef.Importance.MEDIUM,
            "The maximum number of records to put in a single file. "
                + "Must be a non-negative integer number. "
                + "0 is interpreted as \"unlimited\", which is the default.",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.SHORT,
            FILE_MAX_RECORDS
        );

        configDef.define(
            FILE_NAME_TIMESTAMP_TIMEZONE,
            ConfigDef.Type.STRING,
            ZoneOffset.UTC.toString(),
            new TimeZoneValidator(),
            ConfigDef.Importance.LOW,
            "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                + "Use standard shot and long names. Default is UTC",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.SHORT,
            FILE_NAME_TIMESTAMP_TIMEZONE
        );

        configDef.define(
            FILE_NAME_TIMESTAMP_SOURCE,
            ConfigDef.Type.STRING,
            TimestampSource.Type.WALLCLOCK.name(),
            new TimestampSourceValidator(),
            ConfigDef.Importance.LOW,
            "Specifies the the timestamp variable source. Default is wall-clock.",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.SHORT,
            FILE_NAME_TIMESTAMP_SOURCE
        );
    }

    private static void addDeprecatedTimestampConfig(final ConfigDef configDef) {
        int timestampGroupCounter = 0;

        configDef.define(
            TIMESTAMP_TIMEZONE,
            Type.STRING,
            ZoneOffset.UTC.toString(),
            new TimeZoneValidator(),
            Importance.LOW,
            "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                + "Use standard shot and long names. Default is UTC",
            GROUP_FILE,
            timestampGroupCounter++,
            ConfigDef.Width.SHORT,
            TIMESTAMP_TIMEZONE
        );

        configDef.define(
            TIMESTAMP_SOURCE,
            Type.STRING,
            TimestampSource.Type.WALLCLOCK.name(),
            new TimestampSourceValidator(),
            Importance.LOW,
            "Specifies the the timestamp variable source. Default is wall-clock.",
            GROUP_FILE,
            timestampGroupCounter,
            ConfigDef.Width.SHORT,
            TIMESTAMP_SOURCE
        );
    }

    private static void addDeprecatedConfiguration(final ConfigDef configDef) {
        configDef.define(
            AWS_S3_PREFIX_CONFIG,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            Importance.MEDIUM,
            "[Deprecated] Use `file.name.template` instead. Prefix for stored objects, e.g. cluster-1/",
            GROUP_AWS,
            0,
            ConfigDef.Width.NONE,
            AWS_S3_PREFIX_CONFIG
        );

        configDef.define(
            AWS_ACCESS_KEY_ID,
            Type.PASSWORD,
            null,
            new NonEmptyPassword() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    LOGGER.info(AWS_ACCESS_KEY_ID
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, value);
                }
            },
            Importance.MEDIUM,
            "AWS Access Key ID"
        );

        configDef.define(
            AWS_SECRET_ACCESS_KEY,
            Type.PASSWORD,
            null,
            new NonEmptyPassword() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    LOGGER.info(AWS_SECRET_ACCESS_KEY
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, value);
                }
            },
            Importance.MEDIUM,
            "AWS Secret Access Key"
        );

        configDef.define(
            AWS_S3_BUCKET,
            Type.STRING,
            null,
            new BucketNameValidator() {
                @Override
                public void ensureValid(final String name, final Object o) {
                    LOGGER.info(AWS_S3_BUCKET
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, o);
                }
            },
            Importance.MEDIUM,
            "AWS S3 Bucket name"
        );

        configDef.define(
            AWS_S3_ENDPOINT,
            Type.STRING,
            null,
            new UrlValidator() {
                @Override
                public void ensureValid(final String name, final Object o) {
                    LOGGER.info(AWS_S3_ENDPOINT
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, o);
                }
            },
            Importance.LOW,
            "Explicit AWS S3 Endpoint Address, mainly for testing"
        );

        configDef.define(
            AWS_S3_REGION,
            Type.STRING,
            null,
            new AwsRegionValidator() {
                @Override
                public void ensureValid(final String name, final Object o) {
                    LOGGER.info(AWS_S3_REGION
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, o);
                }
            },
            Importance.MEDIUM,
            "AWS S3 Region, e.g. us-east-1"
        );

        configDef.define(
            AWS_S3_PREFIX,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString() {
                @Override
                public void ensureValid(final String name, final Object o) {
                    LOGGER.info(AWS_S3_PREFIX
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, o);
                }
            },
            Importance.MEDIUM,
            "Prefix for stored objects, e.g. cluster-1/"
        );

        configDef.define(
            OUTPUT_FIELDS,
            Type.LIST,
            null,
            new OutputFieldsValidator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    LOGGER.info(OUTPUT_FIELDS
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, value);
                }
            },
            Importance.MEDIUM,
            "Output fields. A comma separated list of one or more: "
                + OUTPUT_FIELD_NAME_KEY + ", "
                + OUTPUT_FIELD_NAME_OFFSET + ", "
                + OUTPUT_FIELD_NAME_TIMESTAMP + ", "
                + OUTPUT_FIELD_NAME_VALUE + ", "
                + OUTPUT_FIELD_NAME_HEADERS
        );

        configDef.define(
            OUTPUT_COMPRESSION,
            Type.STRING,
            null,
            new FileCompressionTypeValidator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    LOGGER.info(OUTPUT_COMPRESSION
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, value);
                }
            },
            Importance.MEDIUM,
            "Output compression. Valid values are: "
                + OUTPUT_COMPRESSION_TYPE_GZIP + " and "
                + OUTPUT_COMPRESSION_TYPE_NONE
        );
    }

    private void validate() {
        final AwsStsRole awsStsRole = getStsRole();

        if (!awsStsRole.isValid()) {
            final AwsAccessSecret awsNewSecret = getNewAwsCredentials();
            if (!awsNewSecret.isValid()) {
                final AwsAccessSecret awsOldSecret = getOldAwsCredentials();
                if (!awsOldSecret.isValid()) {
                    LOGGER.info("Connector use {} as credential Provider, "
                                    + "when configuration for {{}, {}} OR {{}, {}} are absent",
                            AWS_CREDENTIALS_PROVIDER_CONFIG,
                            AWS_ACCESS_KEY_ID_CONFIG, AWS_SECRET_ACCESS_KEY_CONFIG,
                            AWS_STS_ROLE_ARN, AWS_STS_ROLE_SESSION_NAME);
                } else {
                    LOGGER.error(
                        "Config options {} and {} are deprecated",
                        AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
                    );
                }
            }
        } else {
            final AwsStsEndpointConfig stsEndpointConfig = getStsEndpointConfig();
            if (!stsEndpointConfig.isValid()
                && !stsEndpointConfig.getServiceEndpoint().equals(AwsStsEndpointConfig.AWS_STS_GLOBAL_ENDPOINT)) {
                throw new ConfigException(
                    String.format(
                        "%s should be specified together with %s",
                        AWS_S3_REGION_CONFIG, AWS_STS_CONFIG_ENDPOINT)
                );
            }
        }
        if (Objects.isNull(getString(AWS_S3_BUCKET_NAME_CONFIG))
            && Objects.isNull(getString(AWS_S3_BUCKET))) {
            throw new ConfigException(
                String.format(
                    "Neither %s nor %s properties have been set",
                    AWS_S3_BUCKET_NAME_CONFIG,
                    AWS_S3_BUCKET)
            );
        }

    }

    public AwsAccessSecret getOldAwsCredentials() {
        return new AwsAccessSecret(getPassword(AWS_ACCESS_KEY_ID),
            getPassword(AWS_SECRET_ACCESS_KEY));
    }

    public AwsAccessSecret getNewAwsCredentials() {
        return new AwsAccessSecret(getPassword(AWS_ACCESS_KEY_ID_CONFIG),
            getPassword(AWS_SECRET_ACCESS_KEY_CONFIG));
    }

    public AwsAccessSecret getAwsCredentials() {
        return getNewAwsCredentials().isValid()
            ? getNewAwsCredentials()
            : getOldAwsCredentials();
    }

    public String getAwsS3EndPoint() {
        return Objects.nonNull(getString(AWS_S3_ENDPOINT_CONFIG))
            ? getString(AWS_S3_ENDPOINT_CONFIG)
            : getString(AWS_S3_ENDPOINT);
    }

    public Region getAwsS3Region() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        if (Objects.nonNull(getString(AWS_S3_REGION_CONFIG))) {
            return RegionUtils.getRegion(getString(AWS_S3_REGION_CONFIG));
        } else if (Objects.nonNull(getString(AWS_S3_REGION))) {
            return RegionUtils.getRegion(getString(AWS_S3_REGION));
        } else {
            return RegionUtils.getRegion(Regions.US_EAST_1.getName());
        }
    }

    public String getAwsS3BucketName() {
        return Objects.nonNull(getString(AWS_S3_BUCKET_NAME_CONFIG))
            ? getString(AWS_S3_BUCKET_NAME_CONFIG)
            : getString(AWS_S3_BUCKET);
    }

    public String getServerSideEncryptionAlgorithmName() {
        return getString(AWS_S3_SSE_ALGORITHM_CONFIG);
    }

    public String getAwsS3Prefix() {
        return Objects.nonNull(getString(AWS_S3_PREFIX_CONFIG))
            ? getString(AWS_S3_PREFIX_CONFIG)
            : getString(AWS_S3_PREFIX);
    }

    public int getAwsS3PartSize() {
        return getInt(AWS_S3_PART_SIZE);
    }

    public long getS3RetryBackoffDelayMs() {
        return getLong(AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG);
    }

    public long getS3RetryBackoffMaxDelayMs() {
        return getLong(AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
    }

    public int getS3RetryBackoffMaxRetries() {
        return getInt(AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG);
    }

    public CompressionType getCompressionType() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        // default value is GZIP
        if (Objects.nonNull(getString(FILE_COMPRESSION_TYPE_CONFIG))) {
            return CompressionType.forName(getString(FILE_COMPRESSION_TYPE_CONFIG));
        }
        if (Objects.nonNull(getString(OUTPUT_COMPRESSION))) {
            return CompressionType.forName(getString(OUTPUT_COMPRESSION));
        }
        return CompressionType.GZIP;
    }

    public List<OutputField> getOutputFields() {
        if (Objects.nonNull(getList(FORMAT_OUTPUT_FIELDS_CONFIG))
            && get(FORMAT_OUTPUT_FIELDS_CONFIG) != ConfigDef.NO_DEFAULT_VALUE) {
            return getOutputFields(FORMAT_OUTPUT_FIELDS_CONFIG);
        }
        if (Objects.nonNull(getList(OUTPUT_FIELDS))
            && get(OUTPUT_FIELDS) != ConfigDef.NO_DEFAULT_VALUE) {
            return getOutputFields(OUTPUT_FIELDS);
        }
        return List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64));
    }

    public List<OutputField> getOutputFields(final String format) {
        return getList(format).stream()
            .map(fieldName -> {
                final var type = OutputFieldType.forName(fieldName);
                final var encoding =
                    (type == OutputFieldType.KEY || type == OutputFieldType.VALUE)
                        ? getOutputFieldEncodingType()
                        : OutputFieldEncodingType.NONE;
                return new OutputField(type, encoding);
            })
            .collect(Collectors.toUnmodifiableList());
    }

    public OutputFieldEncodingType getOutputFieldEncodingType() {
        return Objects.nonNull(getString(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG))
            ? OutputFieldEncodingType.forName(getString(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG))
            : OutputFieldEncodingType.BASE64;
    }

    public Template getPrefixTemplate() {
        final var t = Template.of(getAwsS3Prefix());
        t.instance()
            .bindVariable(
                "utc_date",
                () -> {
                    LOGGER.info("utc_date variable is deprecated please read documentation for the new name");
                    return "";
                })
            .bindVariable(
                "local_date",
                () -> {
                    LOGGER.info("local_date variable is deprecated please read documentation for the new name");
                    return "";
                })
            .render();
        return t;
    }

    public final ZoneId getTimezone() {
        return ZoneId.of(getString(TIMESTAMP_TIMEZONE));
    }

    public TimestampSource getTimestampSource() {
        return TimestampSource.of(
            getTimezone(),
            TimestampSource.Type.of(getString(TIMESTAMP_SOURCE))
        );
    }

    public AwsStsRole getStsRole() {
        return new AwsStsRole(getString(AWS_STS_ROLE_ARN),
                              getString(AWS_STS_ROLE_EXTERNAL_ID),
                              getString(AWS_STS_ROLE_SESSION_NAME),
                              getInt(AWS_STS_ROLE_SESSION_DURATION));
    }

    public boolean hasAwsStsRole() {
        return getStsRole().isValid();
    }

    public boolean hasStsEndpointConfig() {
        return getStsEndpointConfig().isValid();
    }

    public AwsStsEndpointConfig getStsEndpointConfig() {
        return new AwsStsEndpointConfig(getString(AWS_STS_CONFIG_ENDPOINT),
                                        getString(AWS_S3_REGION_CONFIG));
    }

    protected static class AwsRegionValidator implements ConfigDef.Validator {
        private static final String SUPPORTED_AWS_REGIONS =
            Arrays.stream(Regions.values())
                .map(Regions::getName)
                .collect(Collectors.joining(", "));

        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.nonNull(value)) {
                final String valueStr = (String) value;
                final Region region = RegionUtils.getRegion(valueStr);
                if (!RegionUtils.getRegions().contains(region)) {
                    throw new ConfigException(
                        name, valueStr,
                        "supported values are: " + SUPPORTED_AWS_REGIONS);
                }
            }
        }
    }

    public Boolean usesFileNameTemplate() {
        return Objects.isNull(getString(AWS_S3_PREFIX_CONFIG)) && Objects.isNull(getString(AWS_S3_PREFIX));
    }

    public AWSCredentialsProvider getCustomCredentialsProvider() {
        return getConfiguredInstance(AWS_CREDENTIALS_PROVIDER_CONFIG, AWSCredentialsProvider.class);
    }
}
