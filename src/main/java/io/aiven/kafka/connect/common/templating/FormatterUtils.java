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

package io.aiven.kafka.connect.common.templating;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

public class FormatterUtils {
    public static final BiFunction<SinkRecord, Parameter, String> formatKafkaOffset =
        (sinkRecord, usePaddingParameter) ->
            usePaddingParameter.asBoolean()
                ? String.format("%020d", sinkRecord.kafkaOffset())
                : Long.toString(sinkRecord.kafkaOffset());
    public static final BiFunction<TimestampSource, Parameter, String> formatTimestamp =
        new BiFunction<>() {
            private final Map<String, DateTimeFormatter> fomatterMap =
                Map.of(
                    "YYYY", DateTimeFormatter.ofPattern("YYYY"),
                    "MM", DateTimeFormatter.ofPattern("MM"),
                    "dd", DateTimeFormatter.ofPattern("dd"),
                    "HH", DateTimeFormatter.ofPattern("HH")
                );

            @Override
            public String apply(final TimestampSource timestampSource, final Parameter parameter) {
                return timestampSource.time().format(fomatterMap.get(parameter.value()));
            }

        };

    public static String formatKafkaOffset(final SinkRecord record) {
        return formatKafkaOffset.apply(record, Parameter.of("padding", "true"));
    }

//    public static Function<Parameter, String> createKafkaOffsetBinding(final SinkRecord headRecord) {
//        return usePaddingParameter -> usePaddingParameter.asBoolean()
//                ? String.format("%020d", headRecord.kafkaOffset())
//                : Long.toString(headRecord.kafkaOffset());
//    }

//    public static Function<Parameter, String> createTimestampBinding(final TimestampSource timestampSource) {
//        return new Function<>() {
//            private final Map<String, DateTimeFormatter> timestampFormatterMap =
//                    ImmutableMap.of(
//                            "YYYY", DateTimeFormatter.ofPattern("YYYY"),
//                            "MM", DateTimeFormatter.ofPattern("MM"),
//                            "dd", DateTimeFormatter.ofPattern("dd"),
//                            "HH", DateTimeFormatter.ofPattern("HH")
//                    );
//
//            @Override
//            public String apply(final Parameter parameter) {
//                return timestampSource.time().format(timestampFormatterMap.get(parameter.value()));
//            }
//        };
//    }
}
