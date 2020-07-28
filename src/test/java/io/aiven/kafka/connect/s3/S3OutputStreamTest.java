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

import java.util.Random;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3OutputStreamTest {

    @Test
    public void testAivenKafkaConnectS3OutputStreamTest() {
        final Random generator = new Random();
        final int port = generator.nextInt(10000) + 10000;

        final S3Mock api = new S3Mock.Builder().withPort(port).withInMemoryBackend().build();
        api.start();

        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            "test_key_id",
            "test_secret_key"
        );

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(new EndpointConfiguration("http://localhost:" + port, "us-west-2"));
        builder.withPathStyleAccessEnabled(true);

        final AmazonS3 s3Client = builder.build();
        s3Client.createBucket("test-bucket");

        final S3OutputStream storageSmall =
            new S3OutputStream(s3Client, "test-bucket", "test-key-small");

        final byte[] inputSmall = "small".getBytes();
        storageSmall.write(inputSmall);
        assertFalse(s3Client.doesObjectExist("test-bucket", "test-key-small"));
        storageSmall.flush();
        assertFalse(s3Client.doesObjectExist("test-bucket", "test-key-small"));
        storageSmall.close();
        assertTrue(s3Client.doesObjectExist("test-bucket", "test-key-small"));

        final S3OutputStream storageLarge =
            new S3OutputStream(s3Client, "test-bucket", "test-key-large");
        final byte[] inputLarge = new byte[1024 * 1024 * 10];
        storageLarge.write(inputLarge);
        assertFalse(s3Client.doesObjectExist("test-bucket", "test-key-large"));
        storageLarge.flush();
        assertFalse(s3Client.doesObjectExist("test-bucket", "test-key-large"));
        storageLarge.close();
        assertTrue(s3Client.doesObjectExist("test-bucket", "test-key-large"));

        api.stop();
    }
}
