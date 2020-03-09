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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class AivenKafkaConnectS3OutputStream extends OutputStream {

    private AmazonS3 s3Client;
    private String bucketName;
    private String keyName;
    private byte[] buffer;
    private int bufferLen;
    private int bufferSize;
    AivenKafkaConnectS3MultipartUpload multipartUpload;

    public AivenKafkaConnectS3OutputStream(
        final AmazonS3 s3Client,
        final String bucketName,
        final String keyName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.keyName = keyName;
        this.bufferSize = 32 * 1024;
        this.buffer = new byte[this.bufferSize];
        this.bufferLen = 0;
        this.multipartUpload = null;
    }

    private void expandBuffer(final int dataLength) {
        if (this.bufferSize - this.bufferLen < dataLength) {
            int newBufferSize = this.bufferSize;
            while (newBufferSize - this.bufferLen < dataLength) {
                newBufferSize = newBufferSize * 2;
            }
            final byte[] newBuffer = new byte[newBufferSize];
            System.arraycopy(this.buffer, 0, newBuffer, 0, this.bufferLen);
            this.buffer = newBuffer;
            this.bufferSize = newBufferSize;
        }
    }

    @Override
    public void write(final byte[] data,
                      final int offset,
                      final int len) {
        this.expandBuffer(len);
        System.arraycopy(data, offset, this.buffer, this.bufferLen, len);
        this.bufferLen += len;
    }

    @Override
    public void write(final byte[] data) {
        this.write(data, 0, data.length);
    }

    @Override
    public void write(final int dataByte) {
        this.expandBuffer(1);
        this.buffer[this.bufferLen] = (byte) dataByte;
        this.bufferLen += 1;
    }

    @Override
    public void flush() {
        // flush buffered data to S3, if we have at least the minimum required 5MB for multipart request
        if (this.bufferLen > 5 * 1024 * 1024) {
            if (this.multipartUpload == null) {
                this.multipartUpload =
                    new AivenKafkaConnectS3MultipartUpload(this.s3Client, this.bucketName, this.keyName);
            }
            //FIXME use try-resources here
            final InputStream stream = new ByteArrayInputStream(this.buffer, 0, this.bufferLen);
            this.multipartUpload.uploadPart(stream, this.bufferLen);
            this.bufferLen = 0;
        }
    }

    @Override
    public void close() {
        if (this.bufferLen > 0) {
            final InputStream stream = new ByteArrayInputStream(this.buffer, 0, this.bufferLen);

            if (this.multipartUpload != null) {
                this.multipartUpload.uploadPart(stream, this.bufferLen);
            } else {
                final ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(this.bufferLen);
                this.s3Client.putObject(this.bucketName, this.keyName, stream, metadata);
            }
            this.bufferLen = 0;
        }

        if (this.multipartUpload != null) {
            this.multipartUpload.commit();
            this.multipartUpload = null;
        }
    }
}
