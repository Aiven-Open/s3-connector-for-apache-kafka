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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.config.Variables;
import io.aiven.kafka.connect.common.output.OutputWriter;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.connect.common.templating.FormatterUtils.FORMAT_KAFKA_OFFSET;
import static io.aiven.kafka.connect.common.templating.FormatterUtils.FORMAT_TIMESTAMP;

public class S3StreamWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamWriter.class);

    private final OutputWriter outputWriter;

    private final Template prefixTemplate;

    private final S3SinkConfig config;

    private final AmazonS3 s3Client;

    private final Map<TopicPartition, OutputStream> streams;

    public S3StreamWriter(final S3SinkConfig config) {
        super();
        this.prefixTemplate = config.getPrefixTemplate();
        this.config = config;
        final var awsEndPOntConfig = newEndpointConfiguration(this.config);
        final var s3ClientBuilder =
            AmazonS3ClientBuilder
                .standard()
                .withCredentials(
                    new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(
                            config.getAwsAccessKeyId().value(),
                            config.getAwsSecretKey().value()
                        )
                    )
                );
        if (Objects.isNull(awsEndPOntConfig)) {
            s3ClientBuilder.withRegion(config.getAwsS3Region());
        } else {
            s3ClientBuilder
                .withEndpointConfiguration(awsEndPOntConfig)
                .withPathStyleAccessEnabled(true);
        }
        this.s3Client = s3ClientBuilder.build();
        this.outputWriter =
            OutputWriter
                .builder()
                .addFields(config.getOutputFields())
                .build();
        this.streams = new HashMap<>();
    }

    private AwsClientBuilder.EndpointConfiguration newEndpointConfiguration(final S3SinkConfig config) {
        return Objects.nonNull(config.getAwsS3EndPoint())
            ? new AwsClientBuilder.EndpointConfiguration(config.getAwsS3EndPoint(), config.getAwsS3Region().getName())
            : null;
    }

    public void write(final TopicPartition topicPartition, final SinkRecord record) {
        final var out = streams.computeIfAbsent(topicPartition, ignored -> newStreamFor(record));
        try {
            outputWriter.writeRecord(record, out);
            out.flush();
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    private OutputStream newStreamFor(final SinkRecord record) {
        final var prefix =
            prefixTemplate
                .instance()
                .bindVariable(
                    Variables.TIMESTAMP.name,
                    parameter -> FORMAT_TIMESTAMP.apply(config.getTimestampSource(), parameter)
                )
                .bindVariable(
                    Variables.PARTITION.name,
                    () -> record.kafkaPartition().toString()
                )
                .bindVariable(
                    Variables.START_OFFSET.name,
                    parameter -> FORMAT_KAFKA_OFFSET.apply(record, parameter)
                )
                .bindVariable(FilenameTemplateVariable.TOPIC.name, record::topic)
                .bindVariable(
                    "utc_date",
                    () -> ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE)
                )
                .bindVariable(
                    "local_date",
                    () -> LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
                )
                .render();
        final var key =
            String.format(
                "%s-%s-%s",
                record.topic(),
                record.kafkaPartition(),
                FORMAT_KAFKA_OFFSET.apply(record, VariableTemplatePart.Parameter.of("padding", "true")));
        final var fullKey = config.getCompressionType() == CompressionType.GZIP ? prefix + key + ".gz" : prefix + key;
        final var awsOutputStream = new S3OutputStream(s3Client, config.getAwsS3BucketName(), fullKey);
        try {
            return config.getCompressionType() == CompressionType.GZIP
                ? new GZIPOutputStream(awsOutputStream)
                : awsOutputStream;
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    public void flush(final TopicPartition topicPartition) {
        LOGGER.debug("Flush data for {}", topicPartition);
        closeAndRemove(topicPartition);
    }

    public void close(final TopicPartition topicPartition) {
        LOGGER.info("Close stream for {}", topicPartition);
        closeAndRemove(topicPartition);
    }

    private void closeAndRemove(final TopicPartition topicPartition) {
        try {
            streams.getOrDefault(
                topicPartition,
                OutputStream.nullOutputStream()).close();
            streams.remove(topicPartition);
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    public void closeAll() {
        for (final Map.Entry<TopicPartition, OutputStream> e : streams.entrySet()) {
            close(e.getKey());
        }
    }

}
