import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3Constants;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3MultipartUpload;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkTask;
import io.findify.s3mock.S3Mock;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AivenKafkaConnectS3SinkTaskTest {

    @Test
    public void testAivenKafkaConnectS3SinkTaskTest() throws IOException {
        Random generator = new Random();
        int port = generator.nextInt(10000) + 10000;

        S3Mock api = new S3Mock.Builder().withPort(port).withInMemoryBackend().build();
        api.start();

        Map<String, String> properties = new HashMap<>();
        properties.put(AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID, "test_key_id");
        properties.put(AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY, "test_secret_key");
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_BUCKET, "test-bucket");
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_ENDPOINT, "http://localhost:" + port);
        properties.put(AivenKafkaConnectS3Constants.AWS_S3_REGION, "us-west-2");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION, "gzip");
        properties.put(AivenKafkaConnectS3Constants.OUTPUT_FIELDS, "value,key,timestamp,offset");

        // Pre-create our bucket
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            properties.get(AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID),
            properties.get(AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY)
        );
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(new EndpointConfiguration(
            properties.get(AivenKafkaConnectS3Constants.AWS_S3_ENDPOINT),
            properties.get(AivenKafkaConnectS3Constants.AWS_S3_REGION)
        ));
        builder.withPathStyleAccessEnabled(true);

        AmazonS3 s3Client = builder.build();
        s3Client.createBucket(properties.get(AivenKafkaConnectS3Constants.AWS_S3_BUCKET));

        // Create SinkTask
        AivenKafkaConnectS3SinkTask task = new AivenKafkaConnectS3SinkTask();

        task.start(properties);

        TopicPartition tp = new TopicPartition("test-topic", 0);
        Collection tps = new ArrayList<TopicPartition>();
        tps.add(tp);

        task.open(tps);

        // * Simulate periodical flush() cycle - ensure that data files are written

        // Push batch of records
        List<SinkRecord> records = new ArrayList<>();
        int offset;
        for (offset = 0; offset < 100; offset++) {
            records.add(
                new SinkRecord(
                    "test-topic", 0,
                    Schema.BYTES_SCHEMA, "test-key".getBytes(),
                    Schema.BYTES_SCHEMA, "test-value".getBytes(),
                    offset
                )
            );
        }
        task.put(records);

        assertFalse(s3Client.doesObjectExist(properties.get(AivenKafkaConnectS3Constants.AWS_S3_BUCKET), "test-topic-0-0000000000.gz"));

        // Flush data - this is called by Connect on offset.flush.interval
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        offsets.put(tp, new OffsetAndMetadata(100));
        task.flush(offsets);

        assertTrue(s3Client.doesObjectExist(properties.get(AivenKafkaConnectS3Constants.AWS_S3_BUCKET), "test-topic-0-0000000000.gz"));

        // * Verify that we store data on partition unassignment

        records = new ArrayList<>();
        for (offset = 100; offset < 200; offset++) {
            records.add(
                new SinkRecord(
                    "test-topic", 0,
                    Schema.BYTES_SCHEMA, "test-key".getBytes(),
                    Schema.BYTES_SCHEMA, "test-value".getBytes(),
                    offset
                )
            );
        }
        task.put(records);

        assertFalse(s3Client.doesObjectExist(properties.get(AivenKafkaConnectS3Constants.AWS_S3_BUCKET), "test-topic-0-0000000100.gz"));

        task.close(tps);

        assertTrue(s3Client.doesObjectExist(properties.get(AivenKafkaConnectS3Constants.AWS_S3_BUCKET), "test-topic-0-0000000100.gz"));


        // * Verify that we store data on SinkTask shutdown

        records = new ArrayList<>();
        for (offset = 200; offset < 300; offset++) {
            records.add(
                new SinkRecord(
                    "test-topic", 0,
                    Schema.BYTES_SCHEMA, "test-key".getBytes(),
                    Schema.BYTES_SCHEMA, "test-value".getBytes(),
                    offset
                )
            );
        }
        task.put(records);

        assertFalse(s3Client.doesObjectExist(properties.get(AivenKafkaConnectS3Constants.AWS_S3_BUCKET), "test-topic-0-0000000200.gz"));

        task.stop();

        assertTrue(s3Client.doesObjectExist(properties.get(AivenKafkaConnectS3Constants.AWS_S3_BUCKET), "test-topic-0-0000000200.gz"));

        // cleanup

        api.stop();
    }
}
