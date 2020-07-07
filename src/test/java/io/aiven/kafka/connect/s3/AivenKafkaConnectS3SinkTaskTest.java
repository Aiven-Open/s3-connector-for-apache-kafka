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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.GZIPInputStream;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AivenKafkaConnectS3SinkTaskTest {

    private static final String TEST_BUCKET = "test-bucket";

    private static S3Mock s3Api;
    private static AmazonS3 s3Client;

    private static Map<String, String> commonProperties;

    private final ByteArrayConverter byteArrayConverter = new ByteArrayConverter();

    private Map<String, String> properties;

    @BeforeClass
    public static void setUpClass() {
        final Random generator = new Random();
        final int s3Port = generator.nextInt(10000) + 10000;

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        final Map<String, String> commonPropertiesMutable = new HashMap<>();
        commonPropertiesMutable.put(AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID, "test_key_id");
        commonPropertiesMutable.put(AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY, "test_secret_key");
        commonPropertiesMutable.put(AivenKafkaConnectS3Constants.AWS_S3_BUCKET, TEST_BUCKET);
        commonPropertiesMutable.put(AivenKafkaConnectS3Constants.AWS_S3_ENDPOINT, "http://localhost:" + s3Port);
        commonPropertiesMutable.put(AivenKafkaConnectS3Constants.AWS_S3_REGION, "us-west-2");
        commonProperties = Collections.unmodifiableMap(commonPropertiesMutable);

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                commonProperties.get(AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID),
                commonProperties.get(AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY)
        );
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(new EndpointConfiguration(
                commonProperties.get(AivenKafkaConnectS3Constants.AWS_S3_ENDPOINT),
                commonProperties.get(AivenKafkaConnectS3Constants.AWS_S3_REGION)
        ));
        builder.withPathStyleAccessEnabled(true);

        s3Client = builder.build();
    }

    @AfterClass
    public static void tearDownClass() {
        s3Api.stop();
    }

    @Before
    public void setUp() {
        properties = new HashMap<>(commonProperties);

        s3Client.createBucket(TEST_BUCKET);
    }

    @After
    public void tearDown() {
        s3Client.deleteBucket(TEST_BUCKET);
    }

    @Test
    public void testAivenKafkaConnectS3SinkTaskTest() throws IOException {
        // Create SinkTask
        final AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();

        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset,headers");
        task.start(properties);

        final TopicPartition tp = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        // * Simulate periodical flush() cycle - ensure that data files are written

        // Push batch of records
        Collection<SinkRecord> sinkRecords = createBatchOfRecord(0, 100);
        task.put(sinkRecords);

        assertFalse(s3Client.doesObjectExist(TEST_BUCKET, "test-topic-0-0000000000.gz"));

        // Flush data - this is called by Connect on offset.flush.interval
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        ConnectHeaders extectedConnectHeaders = createTestHeaders();

        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, "test-topic-0-0000000000.gz"));

        S3Object s3Object = s3Client.getObject(TEST_BUCKET, "test-topic-0-0000000000.gz");
        S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(s3ObjectInputStream)))) {
            for (String line; (line = br.readLine()) != null;) {
                String[] parts = line.split(",");
                ConnectHeaders actualConnectHeaders = readHeaders(parts[4]);
                assertTrue(headersEquals(actualConnectHeaders, extectedConnectHeaders));
            }
        }

        // * Verify that we store data on partition unassignment
        task.put(createBatchOfRecord(100, 200));

        assertFalse(s3Client.doesObjectExist(TEST_BUCKET, "test-topic-0-0000000100.gz"));

        task.close(tps);

        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, "test-topic-0-0000000100.gz"));

        // * Verify that we store data on SinkTask shutdown
        task.put(createBatchOfRecord(200, 300));

        assertFalse(s3Client.doesObjectExist(TEST_BUCKET, "test-topic-0-0000000200.gz"));

        task.stop();

        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, "test-topic-0-0000000200.gz"));
    }

    @Test
    public void testS3ConstantPrefix() {
        final AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();

        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_PREFIX, "prefix--");
        task.start(properties);

        final TopicPartition tp = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, "prefix--test-topic-0-0000000000.gz"));
    }

    @Test
    public void testS3UtcDatePrefix() {
        final AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();

        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_PREFIX, "prefix-{{ utc_date }}--");
        task.start(properties);

        final TopicPartition tp = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        final String expectedFileName = String.format("prefix-%s--test-topic-0-0000000000.gz",
                ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE));
        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, expectedFileName));

        task.stop();
    }

    @Test
    public void testS3LocalDatePrefix() {
        final AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();

        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_PREFIX, "prefix-{{ local_date }}--");
        task.start(properties);

        final TopicPartition tp = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        final String expectedFileName = String.format("prefix-%s--test-topic-0-0000000000.gz",
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE));
        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, expectedFileName));

        task.stop();
    }

    private Collection<SinkRecord> createBatchOfRecord(final int offsetFrom, final int offsetTo) {
        final ArrayList<SinkRecord> records = new ArrayList<>();
        for (int offset = offsetFrom; offset < offsetTo; offset++) {
            ConnectHeaders connectHeaders = createTestHeaders();
            final SinkRecord record = new SinkRecord(
                    "test-topic",
                    0,
                    Schema.BYTES_SCHEMA, "test-key".getBytes(),
                    Schema.BYTES_SCHEMA, "test-value".getBytes(),
                    offset,
                    1000L,
                    TimestampType.CREATE_TIME,
                    connectHeaders
            );
            records.add(record);

        }
        return records;
    }

    private ConnectHeaders createTestHeaders() {
        ConnectHeaders connectHeaders = new ConnectHeaders();
        connectHeaders.addBytes("test-header-key-1", "test-header-value-1".getBytes(StandardCharsets.UTF_8));
        connectHeaders.addBytes("test-header-key-2", "test-header-value-2".getBytes(StandardCharsets.UTF_8));
        return connectHeaders;
    }

    private ConnectHeaders readHeaders(String s) {
        final ConnectHeaders connectHeaders = new ConnectHeaders();
        final String[] headers = s.split(";");
        for (final String header : headers) {
            final String[] keyValue = header.split(":");
            final String key = new String(Base64.getDecoder().decode(keyValue[0]), StandardCharsets.UTF_8);
            final byte[] value = Base64.getDecoder().decode(keyValue[1]);
            final SchemaAndValue schemaAndValue = byteArrayConverter.toConnectHeader("topic0", key, value);
            connectHeaders.add(key, schemaAndValue);
        }
        return connectHeaders;
    }

    private boolean headersEquals(final Iterable<Header> h1, final Iterable<Header> h2) {
        final Iterator<Header> h1Iterator = h1.iterator();
        final Iterator<Header> h2Iterator = h2.iterator();
        while (h1Iterator.hasNext() && h2Iterator.hasNext()) {
            final Header header1 = h1Iterator.next();
            final Header header2 = h2Iterator.next();
            if (!Objects.equals(header1.key(), header2.key())) {
                return false;
            }
            if (!Objects.equals(header1.schema().type(), header2.schema().type())) {
                return false;
            }
            if (header1.schema().type() != Schema.Type.BYTES) {
                return false;
            }
            if (header2.schema().type() != Schema.Type.BYTES) {
                return false;
            }
            if (!Arrays.equals((byte[]) header1.value(), (byte[]) header2.value())) {
                return false;
            }
        }
        return !h1Iterator.hasNext() && !h2Iterator.hasNext();
    }
}
