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

package io.aiven.kafka.connect.commons.config;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.commons.templating.Template;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import com.amazonaws.regions.Regions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3SinkConfig extends AbstractConfig {
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
    public static final String OUTPUT_FIELD_NAME_KEY = "key";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_OFFSET = "offset";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_TIMESTAMP = "timestamp";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_VALUE = "value";
    @Deprecated
    public static final String OUTPUT_FIELD_NAME_HEADERS = "headers";

    public static final String TIMESTAMP_TIMEZONE = "timestamp.timezone";
    public static final String TIMESTAMP_SOURCE = "timestamp.source";

    public static final Set<String> OUTPUT_FILED_NAMES = new HashSet<>() {
        {
            add(OUTPUT_FIELD_NAME_KEY);
            add(OUTPUT_FIELD_NAME_OFFSET);
            add(OUTPUT_FIELD_NAME_TIMESTAMP);
            add(OUTPUT_FIELD_NAME_VALUE);
            add(OUTPUT_FIELD_NAME_HEADERS);
        }
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SinkConfig.class);

    //FIXME since we support so far both old style and new style of property names
    //      Importance was set to medium,
    //      as soon we will migrate to new values it must be set to HIGH
    //      same for default value
    private static final String GROUP_AWS = "AWS";

    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    public static final String AWS_S3_BUCKET_NAME_CONFIG = "aws.s3.bucket.name";
    public static final String AWS_S3_ENDPOINT_CONFIG = "aws.s3.endpoint";
    public static final String AWS_S3_PREFIX_CONFIG = "aws.s3.prefix";
    public static final String AWS_S3_REGION_CONFIG = "aws.s3.region";

    private static final String GROUP_FILE = "File";

    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";

    public static final String FILE_NAME_TIMESTAMP_TIMEZONE = "file.name.timestamp.timezone";
    public static final String FILE_NAME_TIMESTAMP_SOURCE = "file.name.timestamp.source";

    private static final String GROUP_FORMAT = "Format";

    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";

    public S3SinkConfig(final Map<String, String> originals) {
        super(configDef(), originals);
        validate();
    }

    public static ConfigDef configDef() {
        final var configDef = new ConfigDef();
        addAwsConfigGroup(configDef);
        addFileConfigGroup(configDef);
        addFormatConfigGroup(configDef);
        addDeprecatedConfiguration(configDef);
        return configDef;
    }

    private static void addAwsConfigGroup(final ConfigDef configDef) {
        int awsGroupCounter = 0;
        configDef.define(
            AWS_ACCESS_KEY_ID_CONFIG,
            Type.PASSWORD,
            null,
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
            Importance.MEDIUM,
            "AWS Secret Access Key",
            GROUP_AWS,
            awsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_SECRET_ACCESS_KEY_CONFIG
        );

        configDef.define(
            AWS_S3_BUCKET_NAME_CONFIG,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            Importance.MEDIUM,
            "AWS S3 Bucket name",
            GROUP_AWS,
            awsGroupCounter++,
            ConfigDef.Width.NONE,
            AWS_S3_BUCKET_NAME_CONFIG
        );

        configDef.define(
            AWS_S3_ENDPOINT_CONFIG,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
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
            AWS_S3_PREFIX_CONFIG,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            Importance.MEDIUM,
            "Prefix for stored objects, e.g. cluster-1/",
            GROUP_AWS,
            awsGroupCounter,
            ConfigDef.Width.NONE,
            AWS_S3_PREFIX_CONFIG
        );
    }

    private static void addFileConfigGroup(final ConfigDef configDef) {
        int fileGroupCounter = 0;
        final String supportedCompressionTypes = CompressionType.names().stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));
        configDef.define(
                FILE_COMPRESSION_TYPE_CONFIG,
                ConfigDef.Type.STRING,
                CompressionType.NONE.name,
                (name, value) -> {
                    assert value instanceof String;
                    final String valueStr = (String) value;
                    if (!CompressionType.names().contains(valueStr)) {
                        throw new ConfigException(
                                FILE_COMPRESSION_TYPE_CONFIG, valueStr,
                                "supported values are: " + supportedCompressionTypes);
                    }
                },
                ConfigDef.Importance.MEDIUM,
                "The compression type used for files put on AWS. "
                        + "The supported values are: " + supportedCompressionTypes + ".",
                GROUP_FILE,
                fileGroupCounter,
                ConfigDef.Width.NONE,
                FILE_COMPRESSION_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(CompressionType.names())
        );
    }

    private static void addFormatConfigGroup(final ConfigDef configDef) {
        int formatGroupCounter = 0;

        final String supportedOutputFields = OutputFieldType.names().stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));
        configDef.define(
                FORMAT_OUTPUT_FIELDS_CONFIG,
                ConfigDef.Type.LIST,
                OutputFieldType.VALUE.name,
                (name, value) -> {
                    assert value instanceof List;
                    @SuppressWarnings("unchecked") final List<String> valueList = (List<String>) value;
                    if (valueList.isEmpty()) {
                        throw new ConfigException(
                                FORMAT_OUTPUT_FIELDS_CONFIG, valueList,
                                "cannot be empty");
                    }
                    for (final String fieldName : valueList) {
                        if (!OutputFieldType.isValidName(fieldName)) {
                            throw new ConfigException(
                                    FORMAT_OUTPUT_FIELDS_CONFIG, value,
                                    "supported values are: " + supportedOutputFields);
                        }
                    }
                },
                ConfigDef.Importance.MEDIUM,
                "Fields to put into output files. "
                        + "The supported values are: " + supportedOutputFields + ".",
                GROUP_FORMAT,
                formatGroupCounter++,
                ConfigDef.Width.NONE,
                FORMAT_OUTPUT_FIELDS_CONFIG,
                FixedSetRecommender.ofSupportedValues(OutputFieldType.names())
        );

        final String supportedValueFieldEncodingTypes = CompressionType.names().stream()
                .map(f -> "'" + f + "'")
                .collect(Collectors.joining(", "));
        configDef.define(
                FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
                ConfigDef.Type.STRING,
                OutputFieldEncodingType.BASE64.name,
                (name, value) -> {
                    assert value instanceof String;
                    final String valueStr = (String) value;
                    if (!OutputFieldEncodingType.names().contains(valueStr)) {
                        throw new ConfigException(
                                FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, valueStr,
                                "supported values are: " + supportedValueFieldEncodingTypes);
                    }
                },
                ConfigDef.Importance.MEDIUM,
                "The type of encoding for the value field. "
                        + "The supported values are: " + supportedOutputFields + ".",
                GROUP_FORMAT,
                formatGroupCounter,
                ConfigDef.Width.NONE,
                FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
                FixedSetRecommender.ofSupportedValues(OutputFieldEncodingType.names())
        );
    }

    private static void addTimestampConfig(final ConfigDef configDef) {
        int timestampGroupCounter = 0;

        configDef.define(
                FILE_NAME_TIMESTAMP_TIMEZONE,
                ConfigDef.Type.STRING,
                ZoneOffset.UTC.toString(),
                (name, value) -> {
                    try {
                        ZoneId.of(value.toString());
                    } catch (final Exception e) {
                        throw new ConfigException(
                                FILE_NAME_TIMESTAMP_TIMEZONE,
                                value,
                                e.getMessage());
                    }
                },
                ConfigDef.Importance.LOW,
                "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                        + "Use standard shot and long names. Default is UTC",
                GROUP_FILE,
                timestampGroupCounter++,
                ConfigDef.Width.SHORT,
                FILE_NAME_TIMESTAMP_TIMEZONE
        );

        configDef.define(
                FILE_NAME_TIMESTAMP_SOURCE, // TIMESTAMP_SOURCE
                ConfigDef.Type.STRING,
                TimestampSource.Type.WALLCLOCK.name(),
                (name, value) -> {
                    try {
                        TimestampSource.Type.of(value.toString());
                    } catch (final Exception e) {
                        throw new ConfigException(
                                FILE_NAME_TIMESTAMP_SOURCE,
                                value,
                                e.getMessage());
                    }
                },
                ConfigDef.Importance.LOW,
                "Specifies the the timestamp variable source. Default is wall-clock.",
                GROUP_FILE,
                timestampGroupCounter,
                ConfigDef.Width.SHORT,
                FILE_NAME_TIMESTAMP_SOURCE
        );
    }

    private static void addDeprecatedConfiguration(final ConfigDef configDef) {
        configDef.define(
            AWS_ACCESS_KEY_ID,
            Type.PASSWORD,
            null,
            (name, value) -> LOGGER.info(AWS_ACCESS_KEY_ID
                    + " property is deprecated please read documentation for the new name"),
            Importance.HIGH,
            "AWS Access Key ID"
        );

        configDef.define(
            AWS_SECRET_ACCESS_KEY,
            Type.PASSWORD,
            null,
            (name, value) -> LOGGER.info(AWS_SECRET_ACCESS_KEY
                    + " property is deprecated please read documentation for the new name"),
            Importance.HIGH,
            "AWS Secret Access Key"
        );

        configDef.define(
            AWS_S3_BUCKET,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString() {
                @Override
                public void ensureValid(final String name, final Object o) {
                    LOGGER.info(AWS_S3_BUCKET
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, o);
                }
            },
            Importance.HIGH,
            "AWS S3 Bucket name"
        );

        configDef.define(
            AWS_S3_ENDPOINT,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString() {
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
            Importance.HIGH,
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
            OutputFieldType.VALUE.name,
            new ConfigDef.NonEmptyString() {
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
            CompressionType.GZIP.name,
            new ConfigDef.NonEmptyString() {
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
        if (Objects.isNull(getPassword(AWS_ACCESS_KEY_ID_CONFIG))
            && Objects.isNull(getPassword(AWS_ACCESS_KEY_ID))) {
            throw new ConfigException(
                String.format(
                    "Neither %s nor %s properties have been set",
                    AWS_ACCESS_KEY_ID_CONFIG,
                    AWS_ACCESS_KEY_ID)
            );
        } else if (Objects.isNull(getPassword(AWS_SECRET_ACCESS_KEY_CONFIG))
            && Objects.isNull(getPassword(AWS_SECRET_ACCESS_KEY))) {
            throw new ConfigException(
                String.format(
                    "Neither %s nor %s properties have been set",
                    AWS_SECRET_ACCESS_KEY_CONFIG,
                    AWS_SECRET_ACCESS_KEY)
            );
        } else if (Objects.isNull(getString(AWS_S3_BUCKET_NAME_CONFIG))
            && Objects.isNull(getString(AWS_S3_BUCKET))) {
            throw new ConfigException(
                String.format(
                    "Neither %s nor %s properties have been set",
                    AWS_S3_BUCKET_NAME_CONFIG,
                    AWS_S3_BUCKET)
            );
        }
    }

    public Password getAwsAccessKeyId() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        return Objects.nonNull(getPassword(AWS_ACCESS_KEY_ID_CONFIG))
            ? getPassword(AWS_ACCESS_KEY_ID_CONFIG)
            : getPassword(AWS_ACCESS_KEY_ID);
    }

    public Password getAwsSecretKey() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        return Objects.nonNull(getPassword(AWS_SECRET_ACCESS_KEY_CONFIG))
            ? getPassword(AWS_SECRET_ACCESS_KEY_CONFIG)
            : getPassword(AWS_SECRET_ACCESS_KEY);
    }

    public String getAwsS3EndPoint() {
        return Objects.nonNull(getString(AWS_S3_ENDPOINT_CONFIG))
            ? getString(AWS_S3_ENDPOINT_CONFIG)
            : getString(AWS_S3_ENDPOINT);
    }

    public Regions getAwsS3Region() {
        // we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        if (Objects.nonNull(getString(AWS_S3_REGION_CONFIG))) {
            return Regions.fromName(getString(AWS_S3_REGION_CONFIG));
        } else if (Objects.nonNull(getString(AWS_S3_REGION))) {
            return Regions.fromName(getString(AWS_S3_REGION));
        } else {
            return Regions.US_EAST_1;
        }
    }

    public String getAwsS3BucketName() {
        return Objects.nonNull(getString(AWS_S3_BUCKET_NAME_CONFIG))
            ? getString(AWS_S3_BUCKET_NAME_CONFIG)
            : getString(AWS_S3_BUCKET);
    }

    public String getAwsS3Prefix() {
        return Objects.nonNull(getString(AWS_S3_PREFIX_CONFIG))
            ? getString(AWS_S3_PREFIX_CONFIG)
            : getString(AWS_S3_PREFIX);
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
        if (Objects.nonNull(getList(FORMAT_OUTPUT_FIELDS_CONFIG))) {
            return getOutputFields(FORMAT_OUTPUT_FIELDS_CONFIG);
        } else if (Objects.nonNull(getList(OUTPUT_FIELDS))) {
            return getOutputFields(OUTPUT_FIELDS);
        } else {
            return List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64));
        }
    }

    public List<OutputField> getOutputFields(String format) {
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
            ? OutputFieldEncodingType.forName(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG)
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

    protected static class AwsRegionValidator implements ConfigDef.Validator {
        private static final String SUPPORTED_AWS_REGIONS =
            Arrays.stream(Regions.values())
                .map(Regions::getName)
                .collect(Collectors.joining(", "));

        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.nonNull(value)) {
                final String valueStr = (String) value;
                try {
                    Regions.fromName(valueStr);
                } catch (final IllegalArgumentException e) {
                    throw new ConfigException(
                        name, valueStr,
                        "supported values are: " + SUPPORTED_AWS_REGIONS);
                }
            }
        }
    }
}
