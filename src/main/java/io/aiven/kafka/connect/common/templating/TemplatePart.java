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

package io.aiven.kafka.connect.common.templating;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.config.Variables;

public class TemplatePart {
    private static final Map<String, DateTimeFormatter> FORMATTER_MAP =
        Map.of(
            "YYYY", DateTimeFormatter.ofPattern("YYYY"),
            "MM", DateTimeFormatter.ofPattern("MM"),
            "dd", DateTimeFormatter.ofPattern("dd"),
            "HH", DateTimeFormatter.ofPattern("HH")
        );

    private final String variableName;

    private final TemplateParameter parameter;

    private final String originalPlaceholder;

    protected TemplatePart(final String originalPlaceholder) {
        this(null, TemplateParameter.EMPTY, originalPlaceholder);
    }

    protected TemplatePart(
        final String variableName,
        final TemplateParameter parameter,
        final String originalPlaceholder) {
        this.variableName = variableName;
        this.parameter = parameter;
        this.originalPlaceholder = originalPlaceholder;
    }

    public String format(final TimestampSource timestampSource,
                         final String kafkaTopic,
                         final int kafkaPartition,
                         final long kafkaOffset,
                         final String sinkRecordKey) {
        if (Variables.TIMESTAMP.name.equals(variableName)) {
            return timestampSource.time().format(FORMATTER_MAP.get(parameter.value()));
        }
        if (Variables.UTC_DATE.name.equals(variableName)) {
            return ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE);
        }
        if (Variables.LOCAL_DATE.name.equals(variableName)) {
            return LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        }
        if (Variables.TOPIC.name.equals(variableName)) {
            return kafkaTopic;
        }
        if (Variables.PARTITION.name.equals(variableName)) {
            return Integer.toString(kafkaPartition);
        }
        if (Variables.START_OFFSET.name.equals(variableName)) {
            return parameter.asBoolean() ? String.format("%020d", kafkaOffset) : Long.toString(kafkaOffset);
        }
        if (FilenameTemplateVariable.KEY.name.equals(variableName)) {
            return sinkRecordKey;
        }
        return originalPlaceholder;
    }

}
