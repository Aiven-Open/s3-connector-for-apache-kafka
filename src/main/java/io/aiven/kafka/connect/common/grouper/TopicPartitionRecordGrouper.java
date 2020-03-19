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

package io.aiven.kafka.connect.common.grouper;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

import com.google.common.collect.ImmutableMap;

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

    private final Integer maxRecordsPerFile;

    private final Map<TopicPartition, SinkRecord> currentHeadRecords = new HashMap<>();

    private final Map<String, List<SinkRecord>> fileBuffers = new HashMap<>();

    private final Function<Parameter, String> setTimestamp;

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
        this.setTimestamp = new Function<Parameter, String>() {

            //FIXME move into commons lib
            private final Map<String, DateTimeFormatter> timestampFormatters =
                ImmutableMap.of(
                    "YYYY", DateTimeFormatter.ofPattern("YYYY"),
                    "MM", DateTimeFormatter.ofPattern("MM"),
                    "dd", DateTimeFormatter.ofPattern("dd"),
                    "HH", DateTimeFormatter.ofPattern("HH")
                );

            @Override
            public String apply(final Parameter parameter) {
                return timestampSource.time().format(timestampFormatters.get(parameter.value()));
            }

        };
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
        //FIXME move into commons lib
        final Function<Parameter, String> setKafkaOffset =
            usePaddingParameter -> usePaddingParameter.asBoolean()
                ? String.format("%020d", headRecord.kafkaOffset())
                : Long.toString(headRecord.kafkaOffset());

        return filenameTemplate.instance()
            .bindVariable(FilenameTemplateVariable.TOPIC.name, tp::topic)
            .bindVariable(
                FilenameTemplateVariable.PARTITION.name,
                () -> Integer.toString(tp.partition())
            )
            .bindVariable(
                FilenameTemplateVariable.START_OFFSET.name,
                setKafkaOffset
            )
            .bindVariable(
                FilenameTemplateVariable.TIMESTAMP.name,
                setTimestamp
            )
            .render();
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
