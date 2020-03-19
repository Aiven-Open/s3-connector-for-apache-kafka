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

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TemplateParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateParser.class);

    private static final Pattern VARIABLE_PATTERN =
        Pattern.compile("\\{\\{\\s*([\\w]+)?(:?)([\\w=]+)?\\s*}}"); // {{ var:foo=bar }}

    private static final Pattern PARAMETER_PATTERN =
        Pattern.compile("([\\w]+)?=?([\\w]+)?"); // foo=bar

    private TemplateParser() {

    }

    public static Pair<List<Pair<String, VariableTemplatePart.Parameter>>, List<TemplatePart>> parse(
        final String template) {
        LOGGER.debug("Parse template: {}", template);

        final ImmutableList.Builder<Pair<String, VariableTemplatePart.Parameter>> variablesAndParametersBuilder =
            ImmutableList.builder();
        final ImmutableList.Builder<TemplatePart> templatePartsBuilder =
            ImmutableList.builder();
        final Matcher m = VARIABLE_PATTERN.matcher(template);

        int position = 0;
        while (m.find()) {
            templatePartsBuilder.add(
                new TextTemplatePart(template.substring(position, m.start()))
            );
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

            final VariableTemplatePart.Parameter p = parseParameter(variable, parameter);
            variablesAndParametersBuilder.add(Pair.of(variable, p));
            templatePartsBuilder.add(new VariableTemplatePart(variable, p, m.group()));
            position = m.end();
        }
        templatePartsBuilder.add(new TextTemplatePart(template.substring(position)));

        return Pair.of(variablesAndParametersBuilder.build(), templatePartsBuilder.build());
    }

    private static VariableTemplatePart.Parameter parseParameter(final String variable, final String parameter) {
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

            return VariableTemplatePart.Parameter.of(name, value);
        } else {
            return VariableTemplatePart.Parameter.EMPTY;
        }
    }

}
