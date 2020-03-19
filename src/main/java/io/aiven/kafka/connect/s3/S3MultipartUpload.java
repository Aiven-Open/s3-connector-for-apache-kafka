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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class S3MultipartUpload {

    private final AmazonS3 s3Client;

    private final String bucketName;

    private final String keyName;

    private final String uploadId;

    private final List<PartETag> partETags;

    private int partIndex;

    public S3MultipartUpload(final AmazonS3 s3Client, final String bucketName, final String keyName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.keyName = keyName;
        this.partETags = new ArrayList<>();
        this.partIndex = 1;

        final InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
        final InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        this.uploadId = initResponse.getUploadId();
    }

    public void uploadPart(final InputStream data, final int len) {
        final UploadPartRequest uploadRequest =
            new UploadPartRequest()
                .withBucketName(this.bucketName)
                .withKey(this.keyName)
                .withUploadId(this.uploadId)
                .withPartNumber(this.partIndex)
                .withInputStream(data)
                .withPartSize(len);

        this.partIndex += 1;
        this.partETags.add(this.s3Client.uploadPart(uploadRequest).getPartETag());
    }

    public void commit() {
        final CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
            this.bucketName,
            this.keyName,
            this.uploadId,
            this.partETags
        );

        this.s3Client.completeMultipartUpload(completeRequest);
    }

    public void abort() {
        final AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(
            this.bucketName,
            this.keyName,
            this.uploadId
        );

        this.s3Client.abortMultipartUpload(abortRequest);
    }
}
