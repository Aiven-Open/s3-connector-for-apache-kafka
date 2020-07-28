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
import java.io.InputStream;
import java.io.OutputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class S3OutputStream extends OutputStream {

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final String keyName;
    private byte[] buffer;
    private int bufferLen;
    private int bufferSize;
    private S3MultipartUpload multipartUpload;

    public S3OutputStream(final AmazonS3 s3Client, final String bucketName, final String keyName) {
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
    public void write(final byte[] data, final int offset, final int len) {
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
                    new S3MultipartUpload(this.s3Client, this.bucketName, this.keyName);
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
