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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.templating.Pair;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.s3.S3SinkConfig;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public final class RecordGrouperFactory {

    public static final String KEY_RECORD = KeyRecordGrouper.class.getName();

    public static final String TOPIC_PARTITION_RECORD = TopicPartitionRecordGrouper.class.getName();

    public static final Map<String, List<Pair<String, Boolean>>> SUPPORTED_VARIABLES =
        ImmutableMap.of(
            TOPIC_PARTITION_RECORD, ImmutableList.of(
                Pair.of(FilenameTemplateVariable.TOPIC.name, true),
                Pair.of(FilenameTemplateVariable.PARTITION.name, true),
                Pair.of(FilenameTemplateVariable.START_OFFSET.name, true),
                Pair.of(FilenameTemplateVariable.TIMESTAMP.name, false)
            ),
            KEY_RECORD, ImmutableList.of(Pair.of(FilenameTemplateVariable.KEY.name, true))
        );

    public static final Set<String> ALL_SUPPORTED_VARIABLES =
        SUPPORTED_VARIABLES.values()
            .stream()
            .flatMap(List::stream)
            .map(Pair::left)
            .collect(Collectors.toSet());
    public static final String SUPPORTED_VARIABLES_LIST =
        SUPPORTED_VARIABLES
            .values().stream()
            .map(v -> String.join(",", v.stream().map(Pair::left).collect(Collectors.toList())))
            .collect(Collectors.joining("; "));
    private static final Set<String> KEY_RECORD_REQUIRED_VARS =
        SUPPORTED_VARIABLES.get(KEY_RECORD).stream()
            .filter(Pair::right)
            .map(Pair::left)
            .collect(Collectors.toSet());
    private static final Set<String> TOPIC_PARTITION_RECORD_REQUIRED_VARS =
        SUPPORTED_VARIABLES.get(TOPIC_PARTITION_RECORD).stream()
            .filter(Pair::right)
            .map(Pair::left)
            .collect(Collectors.toSet());
    private static final Set<String> TOPIC_PARTITION_RECORD_OPT_VARS =
        SUPPORTED_VARIABLES.get(TOPIC_PARTITION_RECORD).stream()
            .filter(p -> !p.right())
            .map(Pair::left)
            .collect(Collectors.toSet());


    private RecordGrouperFactory() {
    }

    public static String resolveRecordGrouperType(final Template template) {
        final Set<String> variables = template.variablesSet();
        if (isByKeyRecord(variables)) {
            return KEY_RECORD;
        } else if (isByTopicPartitionRecord(variables)) {
            return TOPIC_PARTITION_RECORD;
        } else {
            throw new IllegalArgumentException(
                String.format(
                    "unsupported set of template variables, supported sets are: %s",
                    SUPPORTED_VARIABLES_LIST
                )
            );
        }
    }

    public static RecordGrouper newRecordGrouper(final S3SinkConfig sinkConfig) {
        final Template fileNameTemplate = sinkConfig.getFilenameTemplate();
        final String grType = resolveRecordGrouperType(fileNameTemplate);
        if (KEY_RECORD.equals(grType)) {
            return new KeyRecordGrouper(fileNameTemplate);
        } else {
            final Integer maxRecordsPerFile =
                sinkConfig.getMaxRecordsPerFile() != 0
                    ? sinkConfig.getMaxRecordsPerFile()
                    : null;
            return new TopicPartitionRecordGrouper(
                fileNameTemplate,
                maxRecordsPerFile,
                sinkConfig.getFilenameTimestampSource());
        }
    }

    private static boolean isByKeyRecord(final Set<String> vars) {
        return KEY_RECORD_REQUIRED_VARS.equals(vars);
    }

    private static boolean isByTopicPartitionRecord(final Set<String> vars) {
        final Set<String> requiredVars =
            Sets.intersection(TOPIC_PARTITION_RECORD_REQUIRED_VARS, vars)
                .immutableCopy();
        vars.removeAll(requiredVars);
        final boolean containsRequiredVars = TOPIC_PARTITION_RECORD_REQUIRED_VARS.equals(requiredVars);
        final boolean containsOptionalVars =
            vars.isEmpty() || !Collections.disjoint(TOPIC_PARTITION_RECORD_OPT_VARS, vars);
        return containsRequiredVars && containsOptionalVars;
    }

}
