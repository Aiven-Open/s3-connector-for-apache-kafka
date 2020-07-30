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
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TemplateParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateParser.class);

    private static final Pattern VARIABLE_PATTERN =
        Pattern.compile("\\{\\{\\s*([\\w]+)?(:?)([\\w=]+)?\\s*}}"); // {{ var:foo=bar }}

    private static final Pattern PARAMETER_PATTERN =
        Pattern.compile("([\\w]+)?=?([\\w]+)?"); // foo=bar

    public static Pair<List<Pair<String, TemplateParameter>>, List<TemplatePart>> parse(
        final String template) {
        LOGGER.debug("Parse template: {}", template);

        final ImmutableList.Builder<Pair<String, TemplateParameter>> variablesAndParametersBuilder =
            ImmutableList.builder();
        final ImmutableList.Builder<TemplatePart> templatePartsBuilder =
            ImmutableList.builder();
        final Matcher m = VARIABLE_PATTERN.matcher(template);

        int position = 0;
        while (m.find()) {
            templatePartsBuilder.add(new TemplatePart(template.substring(position, m.start())));
            final String variable = m.group(1);
            final String parameterDef = m.group(2);
            final String parameter = m.group(3);

            if (Objects.isNull(variable)) {
                throw new IllegalArgumentException(
                    String.format(
                        "Variable name has't been set for template: %s",
                        template
                    )
                );
            }

            if (parameterDef.equals(":") && Objects.isNull(parameter)) {
                throw new IllegalArgumentException("Wrong variable with parameter definition");
            }

            final TemplateParameter p = parseParameter(variable, parameter);
            variablesAndParametersBuilder.add(Pair.of(variable, p));
            templatePartsBuilder.add(new TemplatePart(variable, p, m.group()));
            position = m.end();
        }
        templatePartsBuilder.add(new TemplatePart(template.substring(position)));

        return Pair.of(variablesAndParametersBuilder.build(), templatePartsBuilder.build());
    }

    private static TemplateParameter parseParameter(final String variable, final String parameter) {
        LOGGER.debug("Parse {} parameter", parameter);
        if (Objects.nonNull(parameter)) {
            final Matcher m = PARAMETER_PATTERN.matcher(parameter);
            if (!m.find()) {
                throw new IllegalArgumentException(
                    String.format(
                        "Parameter hasn't been set for variable `%s`",
                        variable
                    )
                );
            }
            final String name = m.group(1);
            final String value = m.group(2);

            if (Objects.isNull(name)) {
                throw new IllegalArgumentException(
                    String.format(
                        "Parameter name for variable `%s` has not been set",
                        variable
                    )
                );
            }
            if (Objects.isNull(value)) {
                throw new IllegalArgumentException(
                    String.format(
                        "Parameter value for variable `%s` and parameter `%s` has not been set",
                        variable,
                        name
                    )
                );
            }

            return TemplateParameter.of(name, value);
        } else {
            return TemplateParameter.EMPTY;
        }
    }

}
