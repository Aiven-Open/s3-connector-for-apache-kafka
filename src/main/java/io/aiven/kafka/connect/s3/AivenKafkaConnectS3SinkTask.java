package io.aiven.kafka.connect.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AivenKafkaConnectS3SinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(AivenKafkaConnectS3SinkConnector.class);

    private Map<String, String> taskConfig;

    private Base64.Encoder b64Encoder = Base64.getEncoder();
    private Converter keyConverter;
    private Converter valueConverter;

    private AmazonS3 s3Client;

    private Map<TopicPartition, OutputStream> output_streams = new HashMap<TopicPartition, OutputStream>();

    private enum OutputFieldType {
        KEY,
        OFFSET,
        TIMESTAMP,
        VALUE
    }

    OutputFieldType[] output_fields;

    private enum CompressionType {
        GZIP,
        NONE
    }

    CompressionType outputCompression = CompressionType.GZIP;

    @Override
    public String version() {
        return new AivenKafkaConnectS3SinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.taskConfig = new HashMap<>(props);
        this.logger.info("AivenKafkaConnectS3SinkTask starting");

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            props.get(AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID),
            props.get(AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY)
        );
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));

        String region = props.get(AivenKafkaConnectS3Constants.AWS_S3_REGION);
        if (region == null || region.equals("")) {
            region = Regions.US_EAST_1.getName();
        }

        String endpoint_url = props.get(AivenKafkaConnectS3Constants.AWS_S3_ENDPOINT);

        if (endpoint_url == null || endpoint_url.equals("")) {
            builder.withRegion(Regions.fromName(region));
        } else {
            builder.withEndpointConfiguration(new EndpointConfiguration(endpoint_url, region));
            builder.withPathStyleAccessEnabled(true);
        }

        this.s3Client = builder.build();

        this.keyConverter = new ByteArrayConverter();
        this.valueConverter = new ByteArrayConverter();

        String fieldConfig = props.get(AivenKafkaConnectS3Constants.OUTPUT_FIELDS);
        if (fieldConfig == null) {
            fieldConfig = AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_VALUE;
        }
        String[] fieldNames = fieldConfig.split("\\s*,\\s*");
        this.output_fields = new OutputFieldType[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_KEY)) {
                this.output_fields[i] = OutputFieldType.KEY;
            } else if (fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_OFFSET)) {
                this.output_fields[i] = OutputFieldType.OFFSET;
            } else if (fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_TIMESTAMP)) {
                this.output_fields[i] = OutputFieldType.TIMESTAMP;
            } else if (fieldNames[i].equalsIgnoreCase(AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_VALUE)) {
                this.output_fields[i] = OutputFieldType.VALUE;
            } else {
                throw new ConnectException("Unknown output field name '" + fieldNames[i] + "'.");
            }
        }

        String compression = props.get(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION);
        if (compression != null) {
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
        logger.info("AivenKafkaConnectS3SinkTask stopping");
        for (TopicPartition tp: this.output_streams.keySet()) {
            OutputStream stream = this.output_streams.get(tp);
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    this.logger.error("Error closing stream " + tp.topic() + "-" + tp.partition() + ": " + e);
                }
                this.output_streams.remove(tp);
            }
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        // We don't need to do anything here; we'll create the streams on first message on a partition
        for (TopicPartition tp: partitions) {
            this.logger.info("New assignment " + tp.topic() + "#" + tp.partition());
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) throws ConnectException {
        for (TopicPartition tp: partitions) {
            this.logger.info("Unassigned " + tp.topic() + "#" + tp.partition());
            OutputStream stream = this.output_streams.get(tp);
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
                this.output_streams.remove(tp);
            }
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        this.logger.info("Processing " + records.size() + " records");
        for (SinkRecord record: records) {
            String topic = record.topic();
            Integer partition = record.kafkaPartition();
            TopicPartition tp = new TopicPartition(topic, partition);

            // identify or allocate a new output stream for topic/partition combination
            OutputStream stream = this.output_streams.get(tp);
            if (stream == null) {
                String prefix = this.taskConfig.get(AivenKafkaConnectS3Constants.AWS_S3_PREFIX);
                if (prefix == null) {
                   prefix = "";
                }
                String keyName = prefix + topic + "-" + partition + "-" + String.format("%010d", record.kafkaOffset());
                if (this.outputCompression == CompressionType.GZIP) {
                    keyName = keyName + ".gz";
                }
                stream = new AivenKafkaConnectS3OutputStream(this.s3Client, this.taskConfig.get(AivenKafkaConnectS3Constants.AWS_S3_BUCKET), keyName);
                if (this.outputCompression == CompressionType.GZIP) {
                    try {
                        stream = new GZIPOutputStream(stream);
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }
                this.output_streams.put(tp, stream);
            }

            // Create output with the requested fields
            StringBuilder outputRecordBuilder = new StringBuilder(4096);
            for (int i = 0; i < this.output_fields.length; i++) {
                if (i > 0) {
                    outputRecordBuilder.append(",");
                }

                switch (this.output_fields[i]) {
                    case KEY:
                        Object key_raw = record.key();
                        if (key_raw != null) {
                            byte[] key = this.keyConverter.fromConnectData(
                                record.topic(),
                                record.valueSchema(),
                                key_raw
                            );
                            outputRecordBuilder.append(this.b64Encoder.encodeToString(key));
                        }
                        break;
                    case VALUE:
                        Object value_raw = record.value();
                        if (value_raw != null) {
                            byte[] value = this.valueConverter.fromConnectData(
                                record.topic(),
                                record.valueSchema(),
                                value_raw
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
                }
            }
            outputRecordBuilder.append("\n");

            // write output to the topic/partition specific stream
            try {
                stream.write(outputRecordBuilder.toString().getBytes());
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }

        // Send flush signal down the streams, and give opportunity for part uploads
        for (OutputStream stream: this.output_streams.values()) {
            try {
                stream.flush();
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (TopicPartition tp: offsets.keySet()) {
            OutputStream stream = this.output_streams.get(tp);
            if (stream != null) {
                this.logger.info("Flush records for " + tp.topic() + "-" + tp.partition());
                try {
                    stream.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
                this.output_streams.remove(tp);
            }
        }
    }
}
