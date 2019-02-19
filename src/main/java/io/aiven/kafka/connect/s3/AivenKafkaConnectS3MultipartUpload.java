package io.aiven.kafka.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.UploadPartRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AivenKafkaConnectS3MultipartUpload {
    private static final Logger logger = LoggerFactory.getLogger(AivenKafkaConnectS3MultipartUpload.class);
    private AmazonS3 s3;
    private String bucketName;
    private String keyName;
    private String uploadId;
    private List<PartETag> partETags;
    private int partIndex;

    public AivenKafkaConnectS3MultipartUpload(AmazonS3 s3, String bucketName, String keyName) {
        InitiateMultipartUploadRequest initRequest;
        InitiateMultipartUploadResult initResponse;

        this.s3 = s3;
        this.bucketName = bucketName;
        this.keyName = keyName;
        this.partETags = new ArrayList<PartETag>();
        this.partIndex = 1;

        initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
        initResponse = s3.initiateMultipartUpload(initRequest);
        this.uploadId = initResponse.getUploadId();
    }

    public void upload_part(InputStream data, int len) {
        UploadPartRequest uploadRequest = new UploadPartRequest()
            .withBucketName(this.bucketName)
            .withKey(this.keyName)
            .withUploadId(this.uploadId)
            .withPartNumber(this.partIndex)
            .withInputStream(data)
            .withPartSize(len);

        this.partIndex += 1;

        this.partETags.add(this.s3.uploadPart(uploadRequest).getPartETag());
    }

    public void commit() {
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
            this.bucketName,
            this.keyName,
            this.uploadId,
            this.partETags
        );

        this.s3.completeMultipartUpload(completeRequest);
    }

    public void abort() {
        AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(
            this.bucketName,
            this.keyName,
            this.uploadId
        );

        this.s3.abortMultipartUpload(abortRequest);
    }
}
