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

package io.aiven.kafka.connect.common.grouper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatterUtils;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;
import io.aiven.kafka.connect.s3.S3SinkConfig;
import io.aiven.kafka.connect.s3.S3SinkTask;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.Lists;
import io.findify.s3mock.S3Mock;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import static io.aiven.kafka.connect.s3.S3SinkConfig.AWS_ACCESS_KEY_ID;
import static io.aiven.kafka.connect.s3.S3SinkConfig.AWS_S3_BUCKET;
import static io.aiven.kafka.connect.s3.S3SinkConfig.AWS_S3_ENDPOINT;
import static io.aiven.kafka.connect.s3.S3SinkConfig.AWS_S3_REGION;
import static io.aiven.kafka.connect.s3.S3SinkConfig.AWS_SECRET_ACCESS_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is a property-based test for {@link S3SinkTask} (grouping records by the topic and partition)
 * using <a href="https://jqwik.net/docs/current/user-guide.html">jqwik</a>.
 *
 * <p>The idea is to generate random batches of {@link SinkRecord}
 * (see {@link PbtBase#recordBatches()}, put them into a task, and check certain properties
 * of the written files afterwards. Files are written virtually using the in-memory S3 mock.
 */

final class S3SinkTaskGroupByTopicPartitionPropertiesTest extends PbtBase {

    @Property
    final void unlimited(@ForAll("recordBatches") final List<List<SinkRecord>> recordBatches) {
        genericTry(recordBatches, null);
    }

    @Property
    final void limited(@ForAll("recordBatches") final List<List<SinkRecord>> recordBatches,
                       @ForAll @IntRange(min = 1, max = 100) final int maxRecordsPerFile) {
        genericTry(recordBatches, maxRecordsPerFile);
    }

    private void genericTry(final List<List<SinkRecord>> recordBatches,
                            final Integer maxRecordsPerFile) {
        final Random generator = new Random();
        final int s3Port = generator.nextInt(10000) + 10000;

        final S3Mock s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        final Map<String, String> commonPropertiesMutable = new HashMap<>();
        commonPropertiesMutable.put(AWS_ACCESS_KEY_ID, "test_key_id");
        commonPropertiesMutable.put(AWS_SECRET_ACCESS_KEY, "test_secret_key");
        commonPropertiesMutable.put(AWS_S3_BUCKET, TEST_BUCKET);
        commonPropertiesMutable.put(AWS_S3_ENDPOINT, "http://localhost:" + s3Port);
        commonPropertiesMutable.put(AWS_S3_REGION, "us-west-2");
        final Map<String, String> commonProperties = Collections.unmodifiableMap(commonPropertiesMutable);

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
            commonProperties.get(AWS_ACCESS_KEY_ID),
            commonProperties.get(AWS_SECRET_ACCESS_KEY)
        );
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
            commonProperties.get(AWS_S3_ENDPOINT),
            commonProperties.get(AWS_S3_REGION)
        ));
        builder.withPathStyleAccessEnabled(true);

        final AmazonS3 s3Client = builder.build();
        s3Client.createBucket(TEST_BUCKET);

        final BucketAccessor testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET, true);

        final Map<String, String> taskProps = basicTaskProps();
        taskProps.putAll(commonProperties);
        if (maxRecordsPerFile != null) {
            taskProps.put(S3SinkConfig.FILE_MAX_RECORDS, Integer.toString(maxRecordsPerFile));
        }
        final S3SinkTask task = new S3SinkTask();
        task.start(taskProps);

        for (final List<SinkRecord> recordBatch : recordBatches) {
            task.put(recordBatch);
            task.flush(null);
        }

        checkExpectedFileNames(recordBatches, maxRecordsPerFile, testBucketAccessor);
        checkFileSizes(testBucketAccessor, maxRecordsPerFile);
        final int expectedRecordCount = recordBatches.stream().mapToInt(List::size).sum();
        checkTotalRecordCountAndNoMultipleWrites(expectedRecordCount, testBucketAccessor);
        checkTopicPartitionPartInFileNames(testBucketAccessor);
        checkOffsetOrderInFiles(testBucketAccessor);

        s3Api.stop();
        s3Api.shutdown();
    }

    /**
     * Checks that written files have expected names.
     */
    private void checkExpectedFileNames(final List<List<SinkRecord>> recordBatches,
                                        final Integer maxRecordsPerFile,
                                        final BucketAccessor bucketAccessor) {
        final List<String> expectedFileNames = new ArrayList<>();

        for (final List<SinkRecord> recordBatch : recordBatches) {
            final Map<TopicPartition, List<SinkRecord>> groupedPerTopicPartition = recordBatch.stream()
                .collect(
                    Collectors.groupingBy(r -> new TopicPartition(r.topic(), r.kafkaPartition()))
                );

            for (final TopicPartition tp : groupedPerTopicPartition.keySet()) {
                final List<List<SinkRecord>> chunks = Lists.partition(
                    groupedPerTopicPartition.get(tp), effectiveMaxRecordsPerFile(maxRecordsPerFile));
                for (final List<SinkRecord> chunk : chunks) {
                    final SinkRecord record = chunk.get(0);
                    final String blobName = String.format(
                        "%s-%s-%s",
                        record.topic(),
                        record.kafkaPartition(),
                        FormatterUtils.formatKafkaOffset.apply(
                            record, VariableTemplatePart.Parameter.of("padding", "true")
                        )
                    );
                    final String fullBlobName = PREFIX + blobName + CompressionType.GZIP.extension();
                    expectedFileNames.add(fullBlobName);
                }
            }
        }

        assertThat(bucketAccessor.getBlobNames(), containsInAnyOrder(expectedFileNames.toArray()));
    }

    /**
     * For each written file, checks that it's not empty and is not exceeding the maximum size.
     */
    private void checkFileSizes(final BucketAccessor bucketAccessor, final Integer maxRecordsPerFile) {
        final int effectiveMax = effectiveMaxRecordsPerFile(maxRecordsPerFile);
        for (final String filename : bucketAccessor.getBlobNames()) {
            assertThat(
                bucketAccessor.readLines(filename, "gzip"),
                hasSize(allOf(greaterThan(0), lessThanOrEqualTo(effectiveMax)))
            );
        }
    }

    /**
     * Checks, that:
     * <ul>
     * <li>the total number of records written to all files is correct;</li>
     * <li>each record is written only once.</li>
     * </ul>
     */
    private void checkTotalRecordCountAndNoMultipleWrites(final int expectedCount,
                                                          final BucketAccessor bucketAccessor) {
        final Set<String> seenRecords = new HashSet<>();
        for (final String filename : bucketAccessor.getBlobNames()) {
            for (final String line : bucketAccessor.readLines(filename, "gzip")) {
                // Ensure no multiple writes.
                assertFalse(seenRecords.contains(line));
                seenRecords.add(line);
            }
        }
        assertEquals(expectedCount, seenRecords.size());
    }

    /**
     * For each written file, checks that its filename
     * (the topic and partition part) is correct for each record in it.
     */
    private void checkTopicPartitionPartInFileNames(final BucketAccessor bucketAccessor) {
        for (final String filename : bucketAccessor.getBlobNames()) {
            final String filenameWithoutOffset = cutOffsetPart(filename);

            final List<List<String>> lines = bucketAccessor
                .readAndDecodeLines(filename, "gzip", FIELD_KEY, FIELD_VALUE);
            final String firstLineTopicAndPartition = lines.get(0).get(FIELD_VALUE);
            final long firstLineOffset = Long.parseLong(lines.get(0).get(FIELD_OFFSET));
            final String firstLineOffsetWithPadding = String.format("%020d", firstLineOffset);
            final String expectedFilename = PREFIX + firstLineTopicAndPartition + "-"
                + firstLineOffsetWithPadding + CompressionType.GZIP.extension();
            assertEquals(expectedFilename, filename);

            for (final List<String> line : lines) {
                final String value = line.get(FIELD_VALUE);
                assertEquals(PREFIX + value, filenameWithoutOffset);
            }
        }
    }

    /**
     * Cuts off the offset part from a string like "topic-partition-offset".
     */
    private String cutOffsetPart(final String topicPartitionOffset) {
        return topicPartitionOffset.substring(0, topicPartitionOffset.lastIndexOf('-'));
    }

    /**
     * For each written file, checks that offsets of records are increasing.
     */
    private void checkOffsetOrderInFiles(final BucketAccessor bucketAccessor) {
        for (final String filename : bucketAccessor.getBlobNames()) {
            final List<List<String>> lines = bucketAccessor
                .readAndDecodeLines(filename, "gzip", FIELD_KEY, FIELD_VALUE);
            final List<Integer> offsets = lines.stream()
                .map(line -> Integer.parseInt(line.get(FIELD_OFFSET)))
                .collect(Collectors.toList());
            for (int i = 0; i < offsets.size() - 1; i++) {
                assertTrue(offsets.get(i) < offsets.get(i + 1));
            }
        }
    }

    private int effectiveMaxRecordsPerFile(final Integer maxRecordsPerFile) {
        return Objects.requireNonNullElse(maxRecordsPerFile, Integer.MAX_VALUE);
    }
}
