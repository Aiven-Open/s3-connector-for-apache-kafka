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
