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
