package io.aiven.kafka.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AivenKafkaConnectS3OutputStream extends OutputStream {
    private static final Logger logger = LoggerFactory.getLogger(AivenKafkaConnectS3OutputStream.class);
    private AmazonS3 s3Client;
    private String bucketName;
    private String keyName;
    private byte[] buffer;
    private int buffer_len;
    private int buffer_size;
    AivenKafkaConnectS3MultipartUpload multipart_upload;

    public AivenKafkaConnectS3OutputStream(AmazonS3 s3Client, String bucketName, String keyName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.keyName = keyName;
        this.buffer_size = 32 * 1024;
        this.buffer = new byte[this.buffer_size];
        this.buffer_len = 0;
        this.multipart_upload = null;
    }

    private void expand_buffer(int data_length) {
        if (this.buffer_size - this.buffer_len < data_length) {
            byte[] new_buffer;
            int new_buffer_size = this.buffer_size;
            while (new_buffer_size - this.buffer_len < data_length) {
                new_buffer_size = new_buffer_size * 2;
            }
            new_buffer = new byte[new_buffer_size];
            System.arraycopy(this.buffer, 0, new_buffer, 0, this.buffer_len);
            this.buffer = new_buffer;
            this.buffer_size = new_buffer_size;
        }
    }

    @Override
    public void write(byte[] data, int offset, int len) {
        this.expand_buffer(len);
        System.arraycopy(data, offset, this.buffer, this.buffer_len, len);
        this.buffer_len += len;
    }

    @Override
    public void write(byte[] data) {
        this.write(data, 0, data.length);
    }

    @Override
    public void write(int data_byte) {
        this.expand_buffer(1);
        this.buffer[this.buffer_len] = (byte)data_byte;
        this.buffer_len += 1;
    }

    @Override
    public void flush() {
        // flush buffered data to S3, if we have at least the minimum required 5MB for multipart request
        if (this.buffer_len > 5 * 1024 * 1024) {
            if (this.multipart_upload == null) {
                this.multipart_upload = new AivenKafkaConnectS3MultipartUpload(this.s3Client, this.bucketName, this.keyName);
            }
            InputStream stream = new ByteArrayInputStream(this.buffer, 0, this.buffer_len);
            this.multipart_upload.upload_part(stream, this.buffer_len);
            this.buffer_len = 0;
        }
    }

    @Override
    public void close() {
        if (this.buffer_len > 0) {
            InputStream stream = new ByteArrayInputStream(this.buffer, 0, this.buffer_len);

            if (this.multipart_upload != null) {
                this.multipart_upload.upload_part(stream, this.buffer_len);
            } else {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(this.buffer_len);
                this.s3Client.putObject(this.bucketName, this.keyName, stream, metadata);
            }
            this.buffer_len = 0;
        }

        if (this.multipart_upload != null) {
            this.multipart_upload.commit();
            this.multipart_upload = null;
        }
    }
}
