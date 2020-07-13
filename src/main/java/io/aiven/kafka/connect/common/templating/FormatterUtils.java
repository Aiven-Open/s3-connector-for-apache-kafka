package io.aiven.kafka.connect.common.templating;

import com.google.common.collect.ImmutableMap;
import io.aiven.kafka.connect.common.config.TimestampSource;
import org.apache.kafka.connect.sink.SinkRecord;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class FormatterUtils {
    public static final BiFunction<SinkRecord, Parameter, String> formatKafkaOffset =
            (sinkRecord, usePaddingParameter) ->
                    usePaddingParameter.asBoolean()
                            ? String.format("%020d", sinkRecord.kafkaOffset())
                            : Long.toString(sinkRecord.kafkaOffset());

    public static String formatKafkaOffset(final SinkRecord record) {
        return formatKafkaOffset.apply(record, Parameter.of("padding", "true"));
    }

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
