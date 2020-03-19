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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.config.TimestampSource;

import org.apache.commons.lang3.tuple.Pair;

public final class Template {

    private final List<Pair<String, TemplateParameter>> variablesAndParameters;

    private final List<TemplatePart> templateParts;

    private final String originalTemplateString;

    private Template(final String template,
                     final List<Pair<String, TemplateParameter>> variablesAndParameters,
                     final List<TemplatePart> templateParts) {
        this.originalTemplateString = template;
        this.variablesAndParameters = variablesAndParameters;
        this.templateParts = templateParts;
    }

    public static Template of(final String template) {
        final Pair<List<Pair<String, TemplateParameter>>, List<TemplatePart>>
            parsingResult = TemplateParser.parse(template);
        return new Template(template, parsingResult.getLeft(), parsingResult.getRight());
    }

    public final Set<String> variablesSet() {
        return variablesAndParameters.stream().map(Pair::getLeft).collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return originalTemplateString;
    }

    public final String render(final TimestampSource timestampSource,
                               final String kafkaTopic,
                               final int kafkaPartition,
                               final long kafkaOffset,
                               final String recordKey) {
        final StringBuilder sb = new StringBuilder();
        for (final TemplatePart templatePart : templateParts) {
            sb.append(templatePart.format(timestampSource, kafkaTopic, kafkaPartition, kafkaOffset, recordKey));
        }
        return sb.toString();
    }
}
