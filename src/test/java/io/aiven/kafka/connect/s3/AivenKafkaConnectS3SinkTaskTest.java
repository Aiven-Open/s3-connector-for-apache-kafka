package io.aiven.kafka.connect.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AivenKafkaConnectS3SinkTaskTest {

    private static String TEST_BUCKET = "test-bucket";

    private static S3Mock s3Api;
    private static AmazonS3 s3Client;

    private static Map<String, String> commonProperties;

    private Map<String, String> properties;

    @BeforeClass
    public static void setUpClass() {
        Random generator = new Random();
        int s3Port = generator.nextInt(10000) + 10000;

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        Map<String, String> commonPropertiesMutable = new HashMap<>();
        commonPropertiesMutable.put(AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID, "test_key_id");
        commonPropertiesMutable.put(AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY, "test_secret_key");
        commonPropertiesMutable.put(AivenKafkaConnectS3Constants.AWS_S3_BUCKET, TEST_BUCKET);
        commonPropertiesMutable.put(AivenKafkaConnectS3Constants.AWS_S3_ENDPOINT, "http://localhost:" + s3Port);
        commonPropertiesMutable.put(AivenKafkaConnectS3Constants.AWS_S3_REGION, "us-west-2");
        commonProperties = Collections.unmodifiableMap(commonPropertiesMutable);

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
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
    public void testAivenKafkaConnectS3SinkTaskTest() {
        // Create SinkTask
        AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();

        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset");
        task.start(properties);

        TopicPartition tp = new TopicPartition("test-topic", 0);
        Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        // * Simulate periodical flush() cycle - ensure that data files are written

        // Push batch of records
        task.put(createBatchOfRecord(0, 100));

        assertFalse(s3Client.doesObjectExist(TEST_BUCKET, "test-topic-0-0000000000.gz"));

        // Flush data - this is called by Connect on offset.flush.interval
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, "test-topic-0-0000000000.gz"));

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
        AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();

        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_PREFIX, "prefix--");
        task.start(properties);

        TopicPartition tp = new TopicPartition("test-topic", 0);
        Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, "prefix--test-topic-0-0000000000.gz"));
    }

    @Test
    public void testS3UTCDatePrefix() {
        AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();

        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_PREFIX, "prefix-{{ utc_date }}--");
        task.start(properties);

        TopicPartition tp = new TopicPartition("test-topic", 0);
        Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        String expectedFileName = String.format("prefix-%s--test-topic-0-0000000000.gz",
                ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE));
        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, expectedFileName));

        task.stop();
    }
    
    @Test
    public void testS3UTCYearMonthDayPrefix() {
        AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();
        
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_PREFIX, "prefix/{{ utc_year }}/{{ utc_month }}/{{ utc_day }}/");
        task.start(properties);
        
        TopicPartition tp = new TopicPartition("test-topic", 0);
        Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);
        
        task.put(createBatchOfRecord(0, 100));
        
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);
        
        String expectedFileName = String.format("prefix/%s/%s/%s/test-topic-0-0000000000.gz",
            ZonedDateTime.now(ZoneId.of("UTC")).getYear(),
            ZonedDateTime.now(ZoneId.of("UTC")).getMonth(),
            ZonedDateTime.now(ZoneId.of("UTC")).getDayOfMonth());
        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, expectedFileName));
        
        task.stop();
    }

    @Test
    public void testS3LocalDatePrefix() {
        AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();

        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_PREFIX, "prefix-{{ local_date }}--");
        task.start(properties);

        TopicPartition tp = new TopicPartition("test-topic", 0);
        Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);

        task.put(createBatchOfRecord(0, 100));

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        String expectedFileName = String.format("prefix-%s--test-topic-0-0000000000.gz",
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE));
        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, expectedFileName));

        task.stop();
    }
    
    @Test
    public void testS3LocalDateYearMonthDayPrefix() {
        AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();
    
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset");
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_PREFIX, "prefix/{{ utc_year }}/{{ utc_month }}/{{ utc_day }}/");
        task.start(properties);
        
        TopicPartition tp = new TopicPartition("test-topic", 0);
        Collection<TopicPartition> tps = Collections.singletonList(tp);
        task.open(tps);
        
        task.put(createBatchOfRecord(0, 100));
        
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);
    
        String expectedFileName = String.format("prefix/%s/%s/%s/test-topic-0-0000000000.gz",
            LocalDateTime.now().getYear(),
            LocalDateTime.now().getMonth(),
            LocalDateTime.now().getDayOfMonth());
        assertTrue(s3Client.doesObjectExist(TEST_BUCKET, expectedFileName));
        
        task.stop();
    }

    private Collection<SinkRecord> createBatchOfRecord(int offsetFrom, int offsetTo) {
        ArrayList<SinkRecord> records = new ArrayList<>();
        for (int offset = offsetFrom; offset < offsetTo; offset++) {
            SinkRecord record = new SinkRecord(
                    "test-topic",
                    0,
                    Schema.BYTES_SCHEMA, "test-key".getBytes(),
                    Schema.BYTES_SCHEMA, "test-value".getBytes(),
                    offset
            );
            records.add(record);
        }
        return records;
    }
}
