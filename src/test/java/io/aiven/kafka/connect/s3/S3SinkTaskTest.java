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

package io.aiven.kafka.connect.s3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.s3.config.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.config.S3SinkConfig;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.github.luben.zstd.ZstdInputStream;
import com.google.common.collect.Lists;
import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.xerial.snappy.SnappyInputStream;

import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_ACCESS_KEY_ID;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_BUCKET;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_ENDPOINT;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_PREFIX;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_REGION;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_SECRET_ACCESS_KEY;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.OUTPUT_COMPRESSION;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.OUTPUT_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

public class S3SinkTaskTest {

    private static final String TEST_BUCKET = "test-bucket";

    private static S3Mock s3Api;
    private static AmazonS3 s3Client;

    private static Map<String, String> commonProperties;

    private final ByteArrayConverter byteArrayConverter = new ByteArrayConverter();

    private Map<String, String> properties;

    private static BucketAccessor testBucketAccessor;

    @BeforeAll
    public static void setUpClass() {
        final Random generator = new Random();
        final int s3Port = generator.nextInt(10000) + 10000;

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        final Map<String, String> commonPropertiesMutable = new HashMap<>();
        commonPropertiesMutable.put(AWS_ACCESS_KEY_ID, "test_key_id");
        commonPropertiesMutable.put(AWS_SECRET_ACCESS_KEY, "test_secret_key");
        commonPropertiesMutable.put(AWS_S3_BUCKET, TEST_BUCKET);
        commonPropertiesMutable.put(AWS_S3_ENDPOINT, "http://localhost:" + s3Port);
        commonPropertiesMutable.put(AWS_S3_REGION, "us-west-2");
        commonProperties = Collections.unmodifiableMap(commonPropertiesMutable);

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            commonProperties.get(AWS_ACCESS_KEY_ID),
            commonProperties.get(AWS_SECRET_ACCESS_KEY)
        );
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(new EndpointConfiguration(
            commonProperties.get(AWS_S3_ENDPOINT),
            commonProperties.get(AWS_S3_REGION)
        ));
        builder.withPathStyleAccessEnabled(true);

        s3Client = builder.build();

        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET);
        testBucketAccessor.createBucket();
    }

    @AfterAll
    public static void tearDownClass() {
        s3Api.stop();
    }

    @BeforeEach
    public void setUp() {
        properties = new HashMap<>(commonProperties);

        s3Client.createBucket(TEST_BUCKET);
    }

    @AfterEach
    public void tearDown() {
        s3Client.deleteBucket(TEST_BUCKET);
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    public void testAivenKafkaConnectS3SinkTaskTest(final String compression) throws IOException {
        // Create SinkTask
        final S3SinkTask task = new S3SinkTask();

        final CompressionType compressionType = CompressionType.forName(compression);

        properties.put(OUTPUT_FIELDS, "value,key,timestamp,offset,headers");
        properties.put(AWS_S3_PREFIX, "aiven--");
        properties.put(OUTPUT_COMPRESSION, compression);
        task.start(properties);

        final TopicPartition tp = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        // * Simulate periodical flush() cycle - ensure that data files are written

        // Push batch of records
        final Collection<SinkRecord> sinkRecords = createBatchOfRecord(0, 100);
        task.put(sinkRecords);

        assertFalse(s3Client.doesObjectExist(TEST_BUCKET,
            "aiven--test-topic-0-00000000000000000000" + compressionType.extension()));

        // Flush data - this is called by Connect on offset.flush.interval
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        final ConnectHeaders expectedConnectHeaders = createTestHeaders();

        assertTrue(s3Client.doesObjectExist(TEST_BUCKET,
            "aiven--test-topic-0-00000000000000000000" + compressionType.extension()));

        final S3Object s3Object = s3Client.getObject(TEST_BUCKET,
            "aiven--test-topic-0-00000000000000000000" + compressionType.extension());
        final S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
        final InputStream inputStream = getCompressedInputStream(s3ObjectInputStream, compressionType);
        try (final BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            for (String line; (line = br.readLine()) != null;) {
                final String[] parts = line.split(",");
                final ConnectHeaders actualConnectHeaders = readHeaders(parts[4]);
                assertTrue(headersEquals(actualConnectHeaders, expectedConnectHeaders));
            }
        }

        // * Verify that we store data on partition unassignment
        task.put(createBatchOfRecord(100, 200));

        assertFalse(s3Client.doesObjectExist(TEST_BUCKET,
            "aiven--test-topic-0-00000000000000000100" + compressionType.extension()));

        offsets.clear();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        assertTrue(s3Client.doesObjectExist(TEST_BUCKET,
            "aiven--test-topic-0-00000000000000000100" + compressionType.extension()));

        // * Verify that we store data on SinkTask shutdown
        task.put(createBatchOfRecord(200, 300));

        assertFalse(s3Client.doesObjectExist(TEST_BUCKET,
            "aiven--test-topic-0-00000000000000000200" + compressionType.extension()));

        offsets.clear();
        offsets.put(tp, new OffsetAndMetadata(200));
        task.flush(offsets);
        task.stop();

        assertTrue(s3Client.doesObjectExist(TEST_BUCKET,
            "aiven--test-topic-0-00000000000000000200" + compressionType.extension()));
    }

    private InputStream getCompressedInputStream(
        final InputStream inputStream,
        final CompressionType compressionType) throws IOException {
        Objects.requireNonNull(inputStream, "inputStream cannot be null");

        switch (compressionType) {
            case ZSTD:
                return new ZstdInputStream(inputStream);
            case GZIP:
                return new GZIPInputStream(inputStream);
            case SNAPPY:
                return new SnappyInputStream(inputStream);
            default:
                return inputStream;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    public void testS3ConstantPrefix(final String compression) {
        final S3SinkTask task = new S3SinkTask();

        final CompressionType compressionType = CompressionType.forName(compression);

        properties.put(OUTPUT_COMPRESSION, compression);
        properties.put(OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AWS_S3_PREFIX, "prefix--");
        task.start(properties);

        final TopicPartition tp = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        assertTrue(
            s3Client.doesObjectExist(
                TEST_BUCKET,
                "prefix--test-topic-0-00000000000000000000" + compressionType.extension()
            )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    public void testS3UtcDatePrefix(final String compression) {
        final S3SinkTask task = new S3SinkTask();

        final CompressionType compressionType = CompressionType.forName(compression);

        properties.put(OUTPUT_COMPRESSION, compression);
        properties.put(OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AWS_S3_PREFIX, "prefix-{{ utc_date }}--");
        task.start(properties);

        final TopicPartition tp = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        final String expectedFileName = String.format(
            "prefix-%s--test-topic-0-00000000000000000000" + compressionType.extension(),
            ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE));
        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, expectedFileName));

        task.stop();
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    public void testS3LocalDatePrefix(final String compression) {
        final S3SinkTask task = new S3SinkTask();

        final CompressionType compressionType = CompressionType.forName(compression);

        properties.put(OUTPUT_COMPRESSION, compression);
        properties.put(OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AWS_S3_PREFIX, "prefix-{{ local_date }}--");
        task.start(properties);

        final TopicPartition tp = new TopicPartition("test-topic", 0);
        final Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        final String expectedFileName =
            String.format(
                "prefix-%s--test-topic-0-00000000000000000000" + compressionType.extension(),
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
            );
        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, expectedFileName));

        task.stop();
    }

    @Test
    final void failedForStringValuesByDefault() {
        final S3SinkTask task = new S3SinkTask();

        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        properties.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "any_prefix");
        task.start(properties);

        final List<SinkRecord> records = Arrays.asList(
            createRecordWithStringValueSchema("topic0", 0, "key0", "value0", 10, 1000),
            createRecordWithStringValueSchema("topic0", 1, "key1", "value1", 20, 1001),
            createRecordWithStringValueSchema("topic1", 0, "key2", "value2", 30, 1002)

        );

        task.put(records);

        final Throwable t = assertThrows(
            ConnectException.class,
            () -> task.flush(null)
        );
        assertEquals(
            "Record value schema type must be BYTES, STRING given", t.getMessage());
    }

    @Test
    final void supportStringValuesForJsonL() throws IOException {
        final S3SinkTask task = new S3SinkTask();

        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        properties.put(S3SinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "jsonl");
        properties.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "prefix-");
        task.start(properties);

        final List<SinkRecord> records = Arrays.asList(
            createRecordWithStringValueSchema("topic0", 0, "key0", "value0", 10, 1000),
            createRecordWithStringValueSchema("topic0", 1, "key1", "value1", 20, 1001),
            createRecordWithStringValueSchema("topic1", 0, "key2", "value2", 30, 1002)
        );

        final TopicPartition tp00 = new TopicPartition("topic0", 0);
        final TopicPartition tp01 = new TopicPartition("topic0", 1);
        final TopicPartition tp10 = new TopicPartition("topic1", 0);
        final Collection<TopicPartition> tps = List.of(tp00, tp01, tp10);
        task.open(tps);

        task.put(records);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp00, new OffsetAndMetadata(10));
        offsets.put(tp01, new OffsetAndMetadata(20));
        offsets.put(tp10, new OffsetAndMetadata(30));
        task.flush(offsets);

        final CompressionType compressionType = CompressionType.forName(compression);

        final List<String> expectedBlobs = Lists.newArrayList(
                "prefix-topic0-0-00000000000000000010" + compressionType.extension(),
                "prefix-topic0-1-00000000000000000020" + compressionType.extension(),
                "prefix-topic1-0-00000000000000000030" + compressionType.extension()
            );

        for (final String blobName : expectedBlobs) {
            assertTrue(testBucketAccessor.doesObjectExist(blobName));
        }

        assertIterableEquals(
            Arrays.asList("{\"value\":\"value0\",\"key\":\"key0\"}"),
            testBucketAccessor.readLines("prefix-topic0-0-00000000000000000010", compression));
        assertIterableEquals(
            Arrays.asList("{\"value\":\"value1\",\"key\":\"key1\"}"),
            testBucketAccessor.readLines("prefix-topic0-1-00000000000000000020", compression));
        assertIterableEquals(
            Arrays.asList("{\"value\":\"value2\",\"key\":\"key2\"}"),
            testBucketAccessor.readLines("prefix-topic1-0-00000000000000000030", compression));
    }

    @Test
    final void failedForStructValuesByDefault() {
        final S3SinkTask task = new S3SinkTask();
        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        properties.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "prefix-");
        task.start(properties);

        final List<SinkRecord> records = Arrays.asList(
            createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
            createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
            createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002)
        );

        task.put(records);

        final Throwable t = assertThrows(
            ConnectException.class,
            () -> task.flush(null)
        );
        assertEquals(
            "Record value schema type must be BYTES, STRUCT given", t.getMessage());
    }

    @Test
    final void supportStructValuesForJsonL() throws IOException {
        final S3SinkTask task = new S3SinkTask();
        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        properties.put(S3SinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "jsonl");
        properties.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "prefix-");
        task.start(properties);

        final List<SinkRecord> records = Arrays.asList(
            createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
            createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
            createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002)

        );
        final TopicPartition tp00 = new TopicPartition("topic0", 0);
        final TopicPartition tp01 = new TopicPartition("topic0", 1);
        final TopicPartition tp10 = new TopicPartition("topic1", 0);
        final Collection<TopicPartition> tps = List.of(tp00, tp01, tp10);
        task.open(tps);

        task.put(records);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp00, new OffsetAndMetadata(10));
        offsets.put(tp01, new OffsetAndMetadata(20));
        offsets.put(tp10, new OffsetAndMetadata(30));
        task.flush(offsets);

        final CompressionType compressionType = CompressionType.forName(compression);

        final List<String> expectedBlobs = Lists.newArrayList(
            "prefix-topic0-0-00000000000000000010" + compressionType.extension(),
            "prefix-topic0-1-00000000000000000020" + compressionType.extension(),
            "prefix-topic1-0-00000000000000000030" + compressionType.extension()
        );


        for (final String blobName : expectedBlobs) {
            assertTrue(testBucketAccessor.doesObjectExist(blobName));
        }

        assertIterableEquals(
            Arrays.asList("{\"value\":{\"name\":\"name0\"},\"key\":\"key0\"}"),
            testBucketAccessor.readLines("prefix-topic0-0-00000000000000000010", compression));
        assertIterableEquals(
            Arrays.asList("{\"value\":{\"name\":\"name1\"},\"key\":\"key1\"}"),
            testBucketAccessor.readLines("prefix-topic0-1-00000000000000000020", compression));
        assertIterableEquals(
            Arrays.asList("{\"value\":{\"name\":\"name2\"},\"key\":\"key2\"}"),
            testBucketAccessor.readLines("prefix-topic1-0-00000000000000000030", compression));
    }

    @Test
    final void supportStructValuesForClassicJson() throws IOException {
        final S3SinkTask task = new S3SinkTask();
        final String compression = "none";
        properties.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, compression);
        properties.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value");
        properties.put(S3SinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "json");
        task.start(properties);

        final List<SinkRecord> records = Arrays.asList(
            createRecordWithStructValueSchema("topic0", 0, "key0", "name0", 10, 1000),
            createRecordWithStructValueSchema("topic0", 1, "key1", "name1", 20, 1001),
            createRecordWithStructValueSchema("topic1", 0, "key2", "name2", 30, 1002)
        );

        task.put(records);
        task.flush(null);

        final CompressionType compressionType = CompressionType.forName(compression);

        final List<String> expectedBlobs = Lists.newArrayList(
            "topic0-0-10" + compressionType.extension(),
            "topic0-1-20" + compressionType.extension(),
            "topic1-0-30" + compressionType.extension()
        );
        for (final String blobName : expectedBlobs) {
            assertTrue(testBucketAccessor.doesObjectExist(blobName));
        }

        assertIterableEquals(
            Arrays.asList("[", "{\"value\":{\"name\":\"name0\"},\"key\":\"key0\"}", "]"),
            testBucketAccessor.readLines("topic0-0-10", compression));
        assertIterableEquals(
            Arrays.asList("[", "{\"value\":{\"name\":\"name1\"},\"key\":\"key1\"}", "]"),
            testBucketAccessor.readLines("topic0-1-20", compression));
        assertIterableEquals(
            Arrays.asList("[", "{\"value\":{\"name\":\"name2\"},\"key\":\"key2\"}", "]"),
            testBucketAccessor.readLines("topic1-0-30", compression));
    }

    @Test
    final void requestCredentialProviderFromFactoryOnStart() {
        final S3SinkTask task = new S3SinkTask();
        final AwsCredentialProviderFactory mockedFactory = Mockito.mock(AwsCredentialProviderFactory.class);
        final AWSCredentialsProvider provider = Mockito.mock(AWSCredentialsProvider.class);

        task.credentialFactory = mockedFactory;
        Mockito.when(mockedFactory.getProvider(any(S3SinkConfig.class))).thenReturn(provider);

        task.start(properties);

        Mockito.verify(mockedFactory, Mockito.times(1)).getProvider(any(S3SinkConfig.class));
    }

    private SinkRecord createRecordWithStringValueSchema(final String topic,
                                                         final int partition,
                                                         final String key,
                                                         final String value,
                                                         final int offset,
                                                         final long timestamp) {
        return new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            key,
            Schema.STRING_SCHEMA,
            value,
            offset,
            timestamp,
            TimestampType.CREATE_TIME);
    }

    private Collection<SinkRecord> createBatchOfRecord(final int offsetFrom, final int offsetTo) {
        final ArrayList<SinkRecord> records = new ArrayList<>();
        for (int offset = offsetFrom; offset < offsetTo; offset++) {
            final ConnectHeaders connectHeaders = createTestHeaders();
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

    private SinkRecord createRecordWithStructValueSchema(final String topic,
                                                         final int partition,
                                                         final String key,
                                                         final String name,
                                                         final int offset,
                                                         final long timestamp) {
        final Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA);
        final Struct struct = new Struct(schema).put("name", name);
        return new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            key,
            schema,
            struct,
            offset,
            timestamp,
            TimestampType.CREATE_TIME
        );
    }

    private ConnectHeaders createTestHeaders() {
        final ConnectHeaders connectHeaders = new ConnectHeaders();
        connectHeaders.addBytes("test-header-key-1", "test-header-value-1".getBytes(StandardCharsets.UTF_8));
        connectHeaders.addBytes("test-header-key-2", "test-header-value-2".getBytes(StandardCharsets.UTF_8));
        return connectHeaders;
    }

    private ConnectHeaders readHeaders(final String s) {
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
