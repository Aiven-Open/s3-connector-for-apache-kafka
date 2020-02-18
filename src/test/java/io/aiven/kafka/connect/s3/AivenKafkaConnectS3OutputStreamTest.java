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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3OutputStream;
import io.findify.s3mock.S3Mock;
import java.io.IOException;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AivenKafkaConnectS3OutputStreamTest {

    @Test
    public void testAivenKafkaConnectS3OutputStreamTest() throws IOException {
        Random generator = new Random();
        int port = generator.nextInt(10000) + 10000;

        S3Mock api = new S3Mock.Builder().withPort(port).withInMemoryBackend().build();
        api.start();

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            "test_key_id",
            "test_secret_key"
        );

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(new EndpointConfiguration("http://localhost:" + port, "us-west-2"));
        builder.withPathStyleAccessEnabled(true);

        AmazonS3 s3Client = builder.build();

        s3Client.createBucket("test-bucket");

        AivenKafkaConnectS3OutputStream storageSmall = new AivenKafkaConnectS3OutputStream(s3Client, "test-bucket", "test-key-small");

        byte[] inputSmall = "small".getBytes();
        storageSmall.write(inputSmall);
        assertFalse(s3Client.doesObjectExist("test-bucket", "test-key-small"));
        storageSmall.flush();
        assertFalse(s3Client.doesObjectExist("test-bucket", "test-key-small"));
        storageSmall.close();
        assertTrue(s3Client.doesObjectExist("test-bucket", "test-key-small"));

        AivenKafkaConnectS3OutputStream storageLarge = new AivenKafkaConnectS3OutputStream(s3Client, "test-bucket", "test-key-large");
        byte[] inputLarge = new byte[1024*1024*10];
        storageLarge.write(inputLarge);
        assertFalse(s3Client.doesObjectExist("test-bucket", "test-key-large"));
        storageLarge.flush();
        assertFalse(s3Client.doesObjectExist("test-bucket", "test-key-large"));
        storageLarge.close();
        assertTrue(s3Client.doesObjectExist("test-bucket", "test-key-large"));

        api.stop();
    }
}
