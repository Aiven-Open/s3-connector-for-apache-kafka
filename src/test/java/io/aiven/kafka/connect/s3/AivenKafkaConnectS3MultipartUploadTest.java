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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3MultipartUpload;
import io.findify.s3mock.S3Mock;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Random;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class AivenKafkaConnectS3MultipartUploadTest {

    @Test
    public void testAivenKafkaConnectS3MultipartUploadTest() throws IOException {
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

        AivenKafkaConnectS3MultipartUpload mp = new AivenKafkaConnectS3MultipartUpload(
            s3Client,
            "test-bucket",
            "test-object"
        );

        byte[] data = "foobar".getBytes();
        InputStream stream = new ByteArrayInputStream(data, 0, data.length);
        mp.upload_part(stream, data.length);
        mp.commit();

        S3Object object = s3Client.getObject(new GetObjectRequest("test-bucket", "test-object"));
        InputStream objectData = object.getObjectContent();
        assertEquals(objectData.available(), 6);
        byte[] stored_data = new byte[data.length];
        objectData.read(stored_data, 0, data.length);
        assertArrayEquals(data, stored_data);
        objectData.close();

        api.stop();
    }
}
