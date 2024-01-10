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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3OutputStreamTest {

    static final String BUCKET_NAME = "some_bucket";

    static final String FILE_KEY = "some_key";

    static final String UPLOAD_ID = "some_upload_id";

    @Mock
    AmazonS3 mockedAmazonS3;

    @Captor
    ArgumentCaptor<InitiateMultipartUploadRequest> initiateMultipartUploadRequestCaptor;

    @Captor
    ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadRequestCaptor;

    @Captor
    ArgumentCaptor<AbortMultipartUploadRequest> abortMultipartUploadRequestCaptor;

    @Captor
    ArgumentCaptor<UploadPartRequest> uploadPartRequestCaptor;

    final Random random = new Random();

    @Test
    void noRequestsForEmptyBytes() throws Exception {

        try (final var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, 10, mockedAmazonS3)) {
            out.write(new byte[] {});
        }

        verify(mockedAmazonS3, never()).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedAmazonS3, never()).uploadPart(any(UploadPartRequest.class));
        verify(mockedAmazonS3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedAmazonS3, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    void sendsInitialAndCompletionUploadRequests() throws Exception {
        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class)))
            .thenReturn(newUploadPartResult(1, "SOME_ETAG"));
        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(new CompleteMultipartUploadResult());

        try (final var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, 100, mockedAmazonS3)) {
            out.write(1);
        }

        verify(mockedAmazonS3).initiateMultipartUpload(initiateMultipartUploadRequestCaptor.capture());
        verify(mockedAmazonS3).uploadPart(any(UploadPartRequest.class));
        verify(mockedAmazonS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        final var initiateMultipartUploadRequest = initiateMultipartUploadRequestCaptor.getValue();

        assertThat(initiateMultipartUploadRequest.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(initiateMultipartUploadRequest.getKey()).isEqualTo(FILE_KEY);

        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            List.of(new PartETag(1, "SOME_ETAG"))
        );
    }

    @Test
    void sendsAbortForAnyExceptionWhileWriting() {
        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
        doNothing().when(mockedAmazonS3).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class)))
            .thenThrow(RuntimeException.class);

        assertThatThrownBy(() -> {
            try (final var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, 100, mockedAmazonS3)) {
                out.write(new byte[] {1, 2, 3});
            }
        }).isInstanceOf(IOException.class);

        verify(mockedAmazonS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedAmazonS3).uploadPart(any(UploadPartRequest.class));
        verify(mockedAmazonS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void sendsAbortForAnyExceptionWhenClose() throws Exception {
        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
        doNothing().when(mockedAmazonS3).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class)))
            .thenThrow(RuntimeException.class);

        final var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, 10, mockedAmazonS3);

        final var buffer = new byte[5];
        random.nextBytes(buffer);
        out.write(buffer, 0, buffer.length);

        assertThatThrownBy(out::close).isInstanceOf(IOException.class);

        verify(mockedAmazonS3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(mockedAmazonS3).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());

        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void writesOneByte() throws Exception {
        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class)))
            .thenReturn(newUploadPartResult(1, "SOME_ETAG"));
        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(new CompleteMultipartUploadResult());

        try (final var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, 100, mockedAmazonS3)) {
            out.write(1);
        }

        verify(mockedAmazonS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedAmazonS3).uploadPart(uploadPartRequestCaptor.capture());
        verify(mockedAmazonS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        assertUploadPartRequest(
            uploadPartRequestCaptor.getValue(),
            1,
            1,
            new byte[] {1});
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            List.of(new PartETag(1, "SOME_ETAG"))
        );
    }

    @Test
    void writesMultipleMessages() throws Exception {
        final var bufferSize = 10;
        final var message = new byte[bufferSize];

        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class)))
            .thenAnswer(a -> {
                final var up = (UploadPartRequest) a.getArgument(0);
                return newUploadPartResult(up.getPartNumber(), "SOME_TAG#" + up.getPartNumber());
            });
        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(new CompleteMultipartUploadResult());

        final var expectedMessagesList = new ArrayList<byte[]>();
        try (final var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, bufferSize, mockedAmazonS3)) {
            for (int i = 0; i < 3; i++) {
                random.nextBytes(message);
                out.write(message, 0, message.length);
                expectedMessagesList.add(message);
            }
        }

        verify(mockedAmazonS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedAmazonS3, times(3)).uploadPart(uploadPartRequestCaptor.capture());
        verify(mockedAmazonS3).completeMultipartUpload(completeMultipartUploadRequestCaptor.capture());

        final var uploadRequests = uploadPartRequestCaptor.getAllValues();
        var counter = 0;
        for (final var expectedMessage : expectedMessagesList) {
            assertUploadPartRequest(
                uploadRequests.get(counter),
                bufferSize,
                counter + 1,
                expectedMessage);
            counter++;
        }
        assertCompleteMultipartUploadRequest(
            completeMultipartUploadRequestCaptor.getValue(),
            List.of(new PartETag(1, "SOME_TAG#1"),
                new PartETag(2, "SOME_TAG#2"),
                new PartETag(3, "SOME_TAG#3"))
        );
    }

    @Test
    void writesTailMessages() throws Exception {
        final var messageSize = 20;

        final var uploadPartRequests = new ArrayList<UploadPartRequest>();

        when(mockedAmazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
            .thenReturn(newInitiateMultipartUploadResult());
        when(mockedAmazonS3.uploadPart(any(UploadPartRequest.class)))
            .thenAnswer(a -> {
                final var up = (UploadPartRequest) a.getArgument(0);
                //emulate behave of S3 client otherwise we will get wrong arrya in the memory
                up.setInputStream(new ByteArrayInputStream(up.getInputStream().readAllBytes()));
                uploadPartRequests.add(up);

                return newUploadPartResult(up.getPartNumber(), "SOME_TAG#" + up.getPartNumber());
            });
        when(mockedAmazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(new CompleteMultipartUploadResult());

        final var message = new byte[messageSize];

        final var expectedFullMessage = new byte[messageSize + 10];
        final var expectedTailMessage = new byte[10];

        final var out = new S3OutputStream(BUCKET_NAME, FILE_KEY, messageSize + 10, mockedAmazonS3);
        random.nextBytes(message);
        out.write(message);
        System.arraycopy(message, 0, expectedFullMessage, 0, message.length);
        random.nextBytes(message);
        out.write(message);
        System.arraycopy(message, 0, expectedFullMessage, 20, 10);
        System.arraycopy(message, 10, expectedTailMessage, 0, 10);
        out.close();

        assertUploadPartRequest(uploadPartRequests.get(0), 30, 1, expectedFullMessage);
        assertUploadPartRequest(uploadPartRequests.get(1), 10, 2, expectedTailMessage);

        verify(mockedAmazonS3).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        verify(mockedAmazonS3, times(2)).uploadPart(any(UploadPartRequest.class));
        verify(mockedAmazonS3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    private InitiateMultipartUploadResult newInitiateMultipartUploadResult() {
        final var initiateMultipartUploadResult = new InitiateMultipartUploadResult();
        initiateMultipartUploadResult.setUploadId(UPLOAD_ID);
        return initiateMultipartUploadResult;
    }

    private UploadPartResult newUploadPartResult(final int partNumber, final String etag) {
        final var uploadPartResult = new UploadPartResult();
        uploadPartResult.setPartNumber(partNumber);
        uploadPartResult.setETag(etag);
        return uploadPartResult;
    }

    private void assertUploadPartRequest(final UploadPartRequest uploadPartRequest,
                                         final int expectedPartSize,
                                         final int expectedPartNumber,
                                         final byte[] expectedBytes) throws IOException {
        assertThat(uploadPartRequest.getPartSize()).isEqualTo(expectedPartSize);
        assertThat(uploadPartRequest.getUploadId()).isEqualTo(UPLOAD_ID);
        assertThat(uploadPartRequest.getPartNumber()).isEqualTo(expectedPartNumber);
        assertThat(uploadPartRequest.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(uploadPartRequest.getKey()).isEqualTo(FILE_KEY);
        assertThat(uploadPartRequest.getInputStream().readAllBytes()).isEqualTo(expectedBytes);
    }

    private void assertCompleteMultipartUploadRequest(
        final CompleteMultipartUploadRequest completeMultipartUploadRequest,
        final List<PartETag> expectedETags
    ) {
        assertThat(completeMultipartUploadRequest.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(completeMultipartUploadRequest.getKey()).isEqualTo(FILE_KEY);
        assertThat(completeMultipartUploadRequest.getUploadId()).isEqualTo(UPLOAD_ID);
        assertThat(completeMultipartUploadRequest.getPartETags()).hasSameSizeAs(expectedETags);

        assertThat(completeMultipartUploadRequest.getPartETags().stream()
            .collect(Collectors.toMap(PartETag::getPartNumber, PartETag::getETag)))
            .containsAllEntriesOf(expectedETags.stream()
                .collect(Collectors.toMap(PartETag::getPartNumber, PartETag::getETag)));
    }

    private void assertAbortMultipartUploadRequest(final AbortMultipartUploadRequest abortMultipartUploadRequest) {
        assertThat(abortMultipartUploadRequest.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(abortMultipartUploadRequest.getKey()).isEqualTo(FILE_KEY);
        assertThat(abortMultipartUploadRequest.getUploadId()).isEqualTo(UPLOAD_ID);
    }

}
