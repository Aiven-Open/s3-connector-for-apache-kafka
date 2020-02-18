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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import io.aiven.kafka.connect.s3.AivenKafkaConnectS3MultipartUpload;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import io.findify.s3mock.S3Mock;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class AivenKafkaConnectS3MultipartUploadTest {

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

        final AivenKafkaConnectS3MultipartUpload mp = new AivenKafkaConnectS3MultipartUpload(
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
