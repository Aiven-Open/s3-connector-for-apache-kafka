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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class S3MultipartUploadTest {

    @Test
    public void testAivenKafkaConnectS3MultipartUploadTest() throws IOException {
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

        final S3MultipartUpload mp = new S3MultipartUpload(
            s3Client,
            "test-bucket",
            "test-object"
        );

        final byte[] data = "foobar".getBytes();
        final InputStream stream = new ByteArrayInputStream(data, 0, data.length);
        mp.uploadPart(stream, data.length);
        mp.commit();

        final S3Object object = s3Client.getObject(new GetObjectRequest("test-bucket", "test-object"));
        final InputStream objectData = object.getObjectContent();
        assertEquals(objectData.available(), 6);
        final byte[] storedData = new byte[data.length];
        objectData.read(storedData, 0, data.length);
        assertArrayEquals(data, storedData);
        objectData.close();

        api.stop();
    }
}
