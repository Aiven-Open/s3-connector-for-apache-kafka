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

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.s3.templating.TemplatingEngine;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AivenKafkaConnectS3SinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(AivenKafkaConnectS3SinkConnector.class);

    private final ByteArrayConverter byteArrayConverter = new ByteArrayConverter();

    private Map<String, String> taskConfig;

    private Base64.Encoder b64Encoder = Base64.getEncoder();
    private Converter keyConverter;
    private Converter valueConverter;

    private AmazonS3 s3Client;

    private Map<TopicPartition, OutputStream> outputStreams = new HashMap<>();

    private enum OutputFieldType {
        KEY,
        OFFSET,
        TIMESTAMP,
        VALUE,
        HEADERS
    }

    OutputFieldType[] outputFields;

    private enum CompressionType {
        GZIP,
        NONE
    }

    CompressionType outputCompression = CompressionType.GZIP;

    private final TemplatingEngine templatingEngine = new TemplatingEngine();

    {
        templatingEngine.bindVariable("utc_date",
            () -> {
                return ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE);
            }
        );
        templatingEngine.bindVariable("local_date",
            () -> {
                return LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
            }
        );
    }

    @Override
    public String version() {
        return new AivenKafkaConnectS3SinkConnector().version();
    }

    @Override
    public void start(final Map<String, String> props) {
        this.taskConfig = new HashMap<>(props);
        LOGGER.info("AivenKafkaConnectS3SinkTask starting");

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            props.get(AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID),
            props.get(AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY)
        );
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));

        String region = props.get(AivenKafkaConnectS3Constants.AWS_S3_REGION);
        if (region == null || region.equals("")) {
            region = Regions.US_EAST_1.getName();
        }

        final String endpointUrl = props.get(AivenKafkaConnectS3Constants.AWS_S3_ENDPOINT);

        if (endpointUrl == null || endpointUrl.equals("")) {
            builder.withRegion(Regions.fromName(region));
        } else {
            builder.withEndpointConfiguration(new EndpointConfiguration(endpointUrl, region));
            builder.withPathStyleAccessEnabled(true);
        }

        this.s3Client = builder.build();

        this.keyConverter = new ByteArrayConverter();
        this.valueConverter = new ByteArrayConverter();

        String fieldConfig = props.get(AivenKafkaConnectS3Constants.OUTPUT_FIELDS);
        if (fieldConfig == null) {
            fieldConfig = AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_VALUE;
        }
        final String[] fieldNames = fieldConfig.split("\\s*,\\s*");
        this.outputFields = new OutputFieldType[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_KEY)) {
                this.outputFields[i] = OutputFieldType.KEY;
            } else if (fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_OFFSET)) {
                this.outputFields[i] = OutputFieldType.OFFSET;
            } else if (fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_TIMESTAMP)) {
                this.outputFields[i] = OutputFieldType.TIMESTAMP;
            } else if (fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_VALUE)) {
                this.outputFields[i] = OutputFieldType.VALUE;
            } else if (fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_HEADERS)) {
                this.outputFields[i] = OutputFieldType.HEADERS;
            } else {
                throw new ConnectException("Unknown output field name '" + fieldNames[i] + "'.");
            }
        }

        final String compression = props.get(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION);
        if (compression != null) {
            //FIXME simplify if/else statements
            if (compression.equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION_TYPE_GZIP)) {
                // default
            } else if (compression.equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION_TYPE_NONE)) {
                this.outputCompression = CompressionType.NONE;
            } else {
                throw new ConnectException("Unknown output compression type '" + compression + "'.");
            }
        }
    }

    @Override
    public void stop() {
        LOGGER.info("AivenKafkaConnectS3SinkTask stopping");
        for (final TopicPartition tp: this.outputStreams.keySet()) {
            final OutputStream stream = this.outputStreams.get(tp);
            if (stream != null) {
                try {
                    stream.close();
                } catch (final IOException e) {
                    LOGGER.error("Error closing stream " + tp.topic() + "-" + tp.partition() + ": " + e);
                }
                this.outputStreams.remove(tp);
            }
        }
    }

    @Override
    public void open(final Collection<TopicPartition> partitions) {
        // We don't need to do anything here; we'll create the streams on first message on a partition
        for (final TopicPartition tp: partitions) {
            LOGGER.info("New assignment " + tp.topic() + "#" + tp.partition());
        }
    }

    @Override
    public void close(final Collection<TopicPartition> partitions) throws ConnectException {
        for (final TopicPartition tp: partitions) {
            LOGGER.info("Unassigned " + tp.topic() + "#" + tp.partition());
            final OutputStream stream = this.outputStreams.get(tp);
            if (stream != null) {
                try {
                    stream.close();
                } catch (final IOException e) {
                    throw new ConnectException(e);
                }
                this.outputStreams.remove(tp);
            }
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) throws ConnectException {
        LOGGER.info("Processing " + records.size() + " records");
        for (final SinkRecord record: records) {
            final String topic = record.topic();
            final Integer partition = record.kafkaPartition();
            final TopicPartition tp = new TopicPartition(topic, partition);

            // identify or allocate a new output stream for topic/partition combination
            OutputStream stream = this.outputStreams.get(tp);
            if (stream == null) {
                String keyName = getS3Prefix() + topic
                    + "-" + partition
                    + "-" + String.format("%010d", record.kafkaOffset());
                if (this.outputCompression == CompressionType.GZIP) {
                    keyName = keyName + ".gz";
                }
                stream =
                    new AivenKafkaConnectS3OutputStream(
                        this.s3Client,
                        this.taskConfig.get(AivenKafkaConnectS3Constants.AWS_S3_BUCKET),
                        keyName
                    );
                if (this.outputCompression == CompressionType.GZIP) {
                    try {
                        stream = new GZIPOutputStream(stream);
                    } catch (final IOException e) {
                        throw new ConnectException(e);
                    }
                }
                this.outputStreams.put(tp, stream);
            }

            // Create output with the requested fields
            final StringBuilder outputRecordBuilder = new StringBuilder(4096);
            for (int i = 0; i < this.outputFields.length; i++) {
                if (i > 0) {
                    outputRecordBuilder.append(",");
                }

                switch (this.outputFields[i]) {
                    case KEY:
                        final Object keyRaw = record.key();
                        if (keyRaw != null) {
                            final byte[] key = this.keyConverter.fromConnectData(
                                record.topic(),
                                record.valueSchema(),
                                keyRaw
                            );
                            outputRecordBuilder.append(this.b64Encoder.encodeToString(key));
                        }
                        break;
                    case VALUE:
                        final Object valueRaw = record.value();
                        if (valueRaw != null) {
                            final byte[] value = this.valueConverter.fromConnectData(
                                record.topic(),
                                record.valueSchema(),
                                valueRaw
                            );
                            outputRecordBuilder.append(this.b64Encoder.encodeToString(value));
                        }
                        break;
                    case TIMESTAMP:
                        outputRecordBuilder.append(record.timestamp());
                        break;
                    case OFFSET:
                        outputRecordBuilder.append(record.kafkaOffset());
                        break;
                    case HEADERS:
                        writeHeaders(outputRecordBuilder, record);
                        break;
                    default:
                }
            }
            outputRecordBuilder.append("\n");

            // write output to the topic/partition specific stream
            try {
                stream.write(outputRecordBuilder.toString().getBytes());
            } catch (final IOException e) {
                throw new ConnectException(e);
            }
        }

        // Send flush signal down the streams, and give opportunity for part uploads
        for (final OutputStream stream: this.outputStreams.values()) {
            try {
                stream.flush();
            } catch (final IOException e) {
                throw new ConnectException(e);
            }
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (final TopicPartition tp: offsets.keySet()) {
            final OutputStream stream = this.outputStreams.get(tp);
            if (stream != null) {
                LOGGER.info("Flush records for " + tp.topic() + "-" + tp.partition());
                try {
                    stream.close();
                } catch (final IOException e) {
                    throw new ConnectException(e);
                }
                this.outputStreams.remove(tp);
            }
        }
    }

    private String getS3Prefix() {
        final String prefixTemplate = this.taskConfig.get(AivenKafkaConnectS3Constants.AWS_S3_PREFIX);
        if (prefixTemplate == null) {
            return "";
        }
        return templatingEngine.render(prefixTemplate);
    }

    private void writeHeaders(final StringBuilder sb, final SinkRecord record) {
        for (final Header header : record.headers()) {
            final String topic = record.topic();
            final String key = header.key();
            final Object value = header.value();
            final Schema schema = header.schema();
            sb.append(b64Encoder.encodeToString(key.getBytes()));
            sb.append(":");
            final byte[] bytes = byteArrayConverter.fromConnectHeader(topic, key, schema, value);
            sb.append(b64Encoder.encodeToString(bytes));
            sb.append(";");
        }
    }
}
