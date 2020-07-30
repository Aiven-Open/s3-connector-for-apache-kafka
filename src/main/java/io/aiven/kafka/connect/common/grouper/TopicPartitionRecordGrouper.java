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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;

/**
 * A {@link RecordGrouper} that groups records by topic and partition.
 *
 * <p>The class requires a filename template with {@code topic}, {@code partition},
 * and {@code start_offset} variables declared.
 *
 * <p>The class supports limited and unlimited number of records in files.
 */
final class TopicPartitionRecordGrouper implements RecordGrouper {

    private final Template filenameTemplate;

    private final TimestampSource timestampSource;

    private final Integer maxRecordsPerFile;

    private final Map<TopicPartition, SinkRecord> currentHeadRecords = new HashMap<>();

    private final Map<String, List<SinkRecord>> fileBuffers = new HashMap<>();

    /**
     * A constructor.
     *
     * @param filenameTemplate  the filename template.
     * @param maxRecordsPerFile the maximum number of records per file ({@code null} for unlimited).
     */
    public TopicPartitionRecordGrouper(final Template filenameTemplate,
                                       final Integer maxRecordsPerFile,
                                       final TimestampSource timestampSource) {
        Objects.requireNonNull(filenameTemplate, "filenameTemplate cannot be null");
        Objects.requireNonNull(timestampSource, "timestampSource cannot be null");
        this.filenameTemplate = filenameTemplate;
        this.maxRecordsPerFile = maxRecordsPerFile;
        this.timestampSource = timestampSource;
    }

    @Override
    public void put(final SinkRecord record) {
        Objects.requireNonNull(record, "record cannot be null");

        final TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        final SinkRecord currentHeadRecord = currentHeadRecords.computeIfAbsent(tp, ignored -> record);
        final String recordKey = generateRecordKey(tp, currentHeadRecord);

        if (shouldCreateNewFile(recordKey)) {
            // Create new file using this record as the head record.
            currentHeadRecords.put(tp, record);
            final String newRecordKey = generateRecordKey(tp, record);
            fileBuffers.computeIfAbsent(newRecordKey, ignored -> new ArrayList<>()).add(record);
        } else {
            fileBuffers.computeIfAbsent(recordKey, ignored -> new ArrayList<>()).add(record);
        }
    }

    private String generateRecordKey(final TopicPartition tp, final SinkRecord headRecord) {
        return filenameTemplate.render(timestampSource,
            tp.topic(),
            tp.partition(),
            headRecord.kafkaOffset(), null);
    }

    private boolean shouldCreateNewFile(final String recordKey) {
        final boolean unlimited = maxRecordsPerFile == null;
        if (unlimited) {
            return false;
        } else {
            final List<SinkRecord> buffer = fileBuffers.get(recordKey);
            return buffer == null || buffer.size() >= maxRecordsPerFile;
        }
    }

    @Override
    public void clear() {
        currentHeadRecords.clear();
        fileBuffers.clear();
    }

    @Override
    public Map<String, List<SinkRecord>> records() {
        return Collections.unmodifiableMap(fileBuffers);
    }

}
