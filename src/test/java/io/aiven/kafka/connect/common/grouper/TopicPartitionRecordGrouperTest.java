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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.Template;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class TopicPartitionRecordGrouperTest {

    private static final SinkRecord T0P0R0 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 0);
    private static final SinkRecord T0P0R1 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 1);
    private static final SinkRecord T0P0R2 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 2);
    private static final SinkRecord T0P0R3 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 3);
    private static final SinkRecord T0P0R4 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 4);
    private static final SinkRecord T0P0R5 = new SinkRecord(
        "topic0", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 5);

    private static final SinkRecord T0P1R0 = new SinkRecord(
        "topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 10);
    private static final SinkRecord T0P1R1 = new SinkRecord(
        "topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 11);
    private static final SinkRecord T0P1R2 = new SinkRecord(
        "topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 12);
    private static final SinkRecord T0P1R3 = new SinkRecord(
        "topic0", 1, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 13);

    private static final SinkRecord T1P1R0 = new SinkRecord(
        "topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 1000);
    private static final SinkRecord T1P1R1 = new SinkRecord(
        "topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 1001);
    private static final SinkRecord T1P1R2 = new SinkRecord(
        "topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, null, null, null, 1002);
    private static final SinkRecord T1P1R3 = new SinkRecord(
        "topic1", 0, Schema.OPTIONAL_STRING_SCHEMA, "some_key", null, null, 1003);

    private static final TimestampSource DEFAULT_TS_SOURCE =
        TimestampSource.of(TimestampSource.Type.WALLCLOCK);

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = 10)
    final void empty(final Integer maxRecordsPerFile) {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(filenameTemplate, maxRecordsPerFile, DEFAULT_TS_SOURCE);
        assertThat(grouper.records(), anEmptyMap());
    }

    @Test
    final void unlimited() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(filenameTemplate, null, DEFAULT_TS_SOURCE);

        grouper.put(T0P1R0);
        grouper.put(T0P0R0);
        grouper.put(T0P1R1);
        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P1R2);
        grouper.put(T0P0R3);
        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);
        grouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(
            records.keySet(),
            containsInAnyOrder("topic0-0-0", "topic0-1-10", "topic1-0-1000")
        );
        assertThat(records.get("topic0-0-0"),
            contains(T0P0R0, T0P0R1, T0P0R2, T0P0R3, T0P0R4, T0P0R5));
        assertThat(records.get("topic0-1-10"),
            contains(T0P1R0, T0P1R1, T0P1R2, T0P1R3));
        assertThat(records.get("topic1-0-1000"),
            contains(T1P1R0, T1P1R1, T1P1R2, T1P1R3));
    }

    @Test
    final void limited() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(filenameTemplate, 2, DEFAULT_TS_SOURCE);

        grouper.put(T0P1R0);
        grouper.put(T0P0R0);
        grouper.put(T0P1R1);
        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P1R2);
        grouper.put(T0P0R3);
        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);
        grouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(
            records.keySet(),
            containsInAnyOrder(
                "topic0-0-0", "topic0-0-2", "topic0-0-4",
                "topic0-1-10", "topic0-1-12", "topic1-0-1000",
                "topic1-0-1002")
        );
        assertThat(records.get("topic0-0-0"),
            contains(T0P0R0, T0P0R1));
        assertThat(records.get("topic0-0-2"),
            contains(T0P0R2, T0P0R3));
        assertThat(records.get("topic0-0-4"),
            contains(T0P0R4, T0P0R5));
        assertThat(records.get("topic0-1-10"),
            contains(T0P1R0, T0P1R1));
        assertThat(records.get("topic0-1-12"),
            contains(T0P1R2, T0P1R3));
        assertThat(records.get("topic1-0-1000"),
            contains(T1P1R0, T1P1R1));
        assertThat(records.get("topic1-0-1002"),
            contains(T1P1R2, T1P1R3));
    }

    @Test
    final void clear() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{start_offset}}");
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(filenameTemplate, null, DEFAULT_TS_SOURCE);

        grouper.put(T0P1R0);
        grouper.put(T0P0R0);
        grouper.put(T0P1R1);
        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P1R2);
        grouper.put(T0P0R3);
        grouper.put(T0P1R3);

        grouper.clear();
        assertThat(grouper.records(), anEmptyMap());

        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(
            records.keySet(),
            containsInAnyOrder("topic0-0-4", "topic1-0-1000")
        );
        assertThat(records.get("topic0-0-4"),
            contains(T0P0R4, T0P0R5));
        assertThat(records.get("topic1-0-1000"),
            contains(T1P1R0, T1P1R1, T1P1R2, T1P1R3));
    }

    @Test
    final void setZeroPaddingForKafkaOffset() {
        final Template filenameTemplate = Template.of("{{topic}}-{{partition}}-{{start_offset:padding=true}}");
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(filenameTemplate, null, DEFAULT_TS_SOURCE);

        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();
        assertThat(
            records.keySet(),
            containsInAnyOrder(
                "topic0-0-00000000000000000004",
                "topic1-0-00000000000000001000")
        );
        assertThat(
            records.get("topic0-0-00000000000000000004"),
            contains(T0P0R4, T0P0R5)
        );
        assertThat(
            records.get("topic1-0-00000000000000001000"),
            contains(T1P1R0, T1P1R1, T1P1R2, T1P1R3)
        );
    }

    @Test
    final void addTimeUnitsToTheFileName() {
        final Template filenameTemplate =
            Template.of(
                "{{topic}}-"
                    + "{{partition}}-"
                    + "{{start_offset}}-"
                    + "{{timestamp:unit=YYYY}}"
                    + "{{timestamp:unit=MM}}"
                    + "{{timestamp:unit=dd}}"
            );
        final ZonedDateTime t = TimestampSource.of(TimestampSource.Type.WALLCLOCK).time();
        final String expectedTs =
            t.format(DateTimeFormatter.ofPattern("YYYY"))
                + t.format(DateTimeFormatter.ofPattern("MM"))
                + t.format(DateTimeFormatter.ofPattern("dd"));

        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                filenameTemplate,
                null,
                TimestampSource.of(TimestampSource.Type.WALLCLOCK)
            );

        grouper.put(T1P1R0);
        grouper.put(T1P1R1);
        grouper.put(T0P0R4);
        grouper.put(T1P1R2);
        grouper.put(T1P1R3);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertThat(
            records.keySet(),
            containsInAnyOrder(
                "topic0-0-4-" + expectedTs,
                "topic1-0-1000-" + expectedTs
            )
        );
        assertThat(
            records.keySet(),
            containsInAnyOrder(
                "topic0-0-4-" + expectedTs,
                "topic1-0-1000-" + expectedTs
            )
        );
        assertThat(
            records.get("topic0-0-4-" + expectedTs),
            contains(T0P0R4, T0P0R5)
        );
        assertThat(
            records.get("topic1-0-1000-" + expectedTs),
            contains(T1P1R0, T1P1R1, T1P1R2, T1P1R3)
        );
    }

    @Test
    void rotateKeysHourly() {
        final Template filenameTemplate =
            Template.of(
                "{{topic}}-"
                    + "{{partition}}-"
                    + "{{start_offset}}-"
                    + "{{timestamp:unit=YYYY}}"
                    + "{{timestamp:unit=MM}}"
                    + "{{timestamp:unit=dd}}"
                    + "{{timestamp:unit=HH}}"
            );
        final TimestampSource timestampSourceMock = mock(TimestampSource.class);

        final ZonedDateTime firstHourTime = ZonedDateTime.now();
        final ZonedDateTime secondHourTime = firstHourTime.plusHours(1);
        final String firstHourTs =
            firstHourTime.format(DateTimeFormatter.ofPattern("YYYY"))
                + firstHourTime.format(DateTimeFormatter.ofPattern("MM"))
                + firstHourTime.format(DateTimeFormatter.ofPattern("dd"))
                + firstHourTime.format(DateTimeFormatter.ofPattern("HH"));
        final String secondHourTs =
            secondHourTime.format(DateTimeFormatter.ofPattern("YYYY"))
                + secondHourTime.format(DateTimeFormatter.ofPattern("MM"))
                + secondHourTime.format(DateTimeFormatter.ofPattern("dd"))
                + secondHourTime.format(DateTimeFormatter.ofPattern("HH"));

        when(timestampSourceMock.time()).thenReturn(firstHourTime);
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                filenameTemplate,
                null,
                timestampSourceMock
            );

        grouper.put(T0P0R1);
        grouper.put(T0P0R2);
        grouper.put(T0P0R3);

        when(timestampSourceMock.time()).thenReturn(secondHourTime);

        grouper.put(T0P0R4);
        grouper.put(T0P0R5);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertEquals(2, records.size());

        assertThat(
            records.keySet(),
            containsInAnyOrder(
                "topic0-0-1-" + firstHourTs,
                "topic0-0-1-" + secondHourTs
            )
        );
        assertThat(
            records.get("topic0-0-1-" + firstHourTs),
            contains(T0P0R1, T0P0R2, T0P0R3)
        );
        assertThat(
            records.get("topic0-0-1-" + secondHourTs),
            contains(T0P0R4, T0P0R5)
        );
    }

    @Test
    void rotateKeysDaily() {
        final Template filenameTemplate =
            Template.of(
                "{{topic}}-"
                    + "{{partition}}-"
                    + "{{start_offset}}-"
                    + "{{timestamp:unit=YYYY}}"
                    + "{{timestamp:unit=MM}}"
                    + "{{timestamp:unit=dd}}"
            );
        final TimestampSource timestampSourceMock = mock(TimestampSource.class);

        final ZonedDateTime firstDayTime = ZonedDateTime.now();
        final ZonedDateTime secondDayTime = firstDayTime.plusDays(1);
        final String firstDayTs =
            firstDayTime.format(DateTimeFormatter.ofPattern("YYYY"))
                + firstDayTime.format(DateTimeFormatter.ofPattern("MM"))
                + firstDayTime.format(DateTimeFormatter.ofPattern("dd"));
        final String secondDayTs =
            secondDayTime.format(DateTimeFormatter.ofPattern("YYYY"))
                + secondDayTime.format(DateTimeFormatter.ofPattern("MM"))
                + secondDayTime.format(DateTimeFormatter.ofPattern("dd"));

        when(timestampSourceMock.time()).thenReturn(firstDayTime);
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                filenameTemplate,
                null,
                timestampSourceMock
            );

        grouper.put(T0P1R0);
        grouper.put(T0P1R1);
        grouper.put(T0P1R2);

        when(timestampSourceMock.time()).thenReturn(secondDayTime);

        grouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertEquals(2, records.size());

        assertThat(
            records.keySet(),
            containsInAnyOrder(
                "topic0-1-10-" + firstDayTs,
                "topic0-1-10-" + secondDayTs
            )
        );
        assertThat(
            records.get("topic0-1-10-" + firstDayTs),
            contains(T0P1R0, T0P1R1, T0P1R2)
        );
        assertThat(
            records.get("topic0-1-10-" + secondDayTs),
            contains(T0P1R3)
        );
    }

    @Test
    void rotateKeysMonthly() {
        final Template filenameTemplate =
            Template.of(
                "{{topic}}-"
                    + "{{partition}}-"
                    + "{{start_offset}}-"
                    + "{{timestamp:unit=YYYY}}"
                    + "{{timestamp:unit=MM}}"
            );
        final TimestampSource timestampSourceMock = mock(TimestampSource.class);

        final ZonedDateTime firstMonthTime = ZonedDateTime.now().with(TemporalAdjusters.lastDayOfMonth());
        final ZonedDateTime secondMonth = firstMonthTime.plusDays(1);
        final String firstMonthTs =
            firstMonthTime.format(DateTimeFormatter.ofPattern("YYYY"))
                + firstMonthTime.format(DateTimeFormatter.ofPattern("MM"));
        final String secondMonthTs =
            secondMonth.format(DateTimeFormatter.ofPattern("YYYY"))
                + secondMonth.format(DateTimeFormatter.ofPattern("MM"));

        when(timestampSourceMock.time()).thenReturn(firstMonthTime);
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                filenameTemplate,
                null,
                timestampSourceMock
            );

        grouper.put(T0P1R0);
        grouper.put(T0P1R1);
        grouper.put(T0P1R2);

        when(timestampSourceMock.time()).thenReturn(secondMonth);

        grouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertEquals(2, records.size());

        assertThat(
            records.keySet(),
            containsInAnyOrder(
                "topic0-1-10-" + firstMonthTs,
                "topic0-1-10-" + secondMonthTs
            )
        );
        assertThat(
            records.get("topic0-1-10-" + firstMonthTs),
            contains(T0P1R0, T0P1R1, T0P1R2)
        );
        assertThat(
            records.get("topic0-1-10-" + secondMonthTs),
            contains(T0P1R3)
        );
    }

    @Test
    void rotateKeysYearly() {
        final Template filenameTemplate =
            Template.of(
                "{{topic}}-"
                    + "{{partition}}-"
                    + "{{start_offset}}-"
                    + "{{timestamp:unit=YYYY}}"
                    + "{{timestamp:unit=MM}}"
            );
        final TimestampSource timestampSourceMock = mock(TimestampSource.class);

        final ZonedDateTime firstYearTime = ZonedDateTime.now();
        final ZonedDateTime secondYearMonth = firstYearTime.plusYears(1);
        final String firstYearTs =
            firstYearTime.format(DateTimeFormatter.ofPattern("YYYY"))
                + firstYearTime.format(DateTimeFormatter.ofPattern("MM"));
        final String secondYearTs =
            secondYearMonth.format(DateTimeFormatter.ofPattern("YYYY"))
                + secondYearMonth.format(DateTimeFormatter.ofPattern("MM"));

        when(timestampSourceMock.time()).thenReturn(firstYearTime);
        final TopicPartitionRecordGrouper grouper =
            new TopicPartitionRecordGrouper(
                filenameTemplate,
                null,
                timestampSourceMock
            );

        grouper.put(T0P1R0);
        grouper.put(T0P1R1);
        grouper.put(T0P1R2);

        when(timestampSourceMock.time()).thenReturn(secondYearMonth);

        grouper.put(T0P1R3);

        final Map<String, List<SinkRecord>> records = grouper.records();

        assertEquals(2, records.size());

        assertThat(
            records.keySet(),
            containsInAnyOrder(
                "topic0-1-10-" + firstYearTs,
                "topic0-1-10-" + secondYearTs
            )
        );
        assertThat(
            records.get("topic0-1-10-" + firstYearTs),
            contains(T0P1R0, T0P1R1, T0P1R2)
        );
        assertThat(
            records.get("topic0-1-10-" + secondYearTs),
            contains(T0P1R3)
        );
    }

}
