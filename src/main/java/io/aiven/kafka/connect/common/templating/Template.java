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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class Template {

    private static final Pattern VARIABLE_PATTERN =
        Pattern.compile("\\{\\{\\s*(([\\w_]+)(:([\\w]+)=([\\w]+))?)\\s*}}");

    private final List<Pair<String, VariableTemplatePart.Parameter>> variablesAndParameters;

    private final List<TemplatePart> templateParts;

    private final String originalTemplateString;

    private Template(final String template,
                     final List<Pair<String, VariableTemplatePart.Parameter>> variablesAndParameters,
                     final List<TemplatePart> templateParts) {
        this.originalTemplateString = template;
        this.variablesAndParameters = variablesAndParameters;
        this.templateParts = templateParts;
    }

    public static Template of(final String template) {

        final Pair<List<Pair<String, VariableTemplatePart.Parameter>>, List<TemplatePart>>
            parsingResult = TemplateParser.parse(template);

        return new Template(template, parsingResult.left(), parsingResult.right());
    }

    public String originalTemplate() {
        return originalTemplateString;
    }

    public final List<String> variables() {
        return variablesAndParameters.stream()
            .map(Pair::left)
            .collect(Collectors.toList());
    }

    public final Set<String> variablesSet() {
        return variablesAndParameters.stream().map(Pair::left).collect(Collectors.toSet());
    }

    public final List<Pair<String, VariableTemplatePart.Parameter>> variablesWithParameters() {
        return variablesAndParameters;
    }

    public final List<Pair<String, VariableTemplatePart.Parameter>> variablesWithNonEmptyParameters() {
        return variablesAndParameters.stream()
            .filter(e -> !e.right().isEmpty())
            .collect(Collectors.toList());
    }

    public final Instance instance() {
        return new Instance();
    }

    @Override
    public String toString() {
        return originalTemplateString;
    }

    public final class Instance {
        private final Map<String, Function<VariableTemplatePart.Parameter, String>> bindings = new HashMap<>();

        private Instance() {
        }

        public final Instance bindVariable(final String name, final Supplier<String> binding) {
            return bindVariable(name, x -> binding.get());
        }

        public final Instance bindVariable(final String name,
                                           final Function<VariableTemplatePart.Parameter, String> binding) {
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(binding, "binding cannot be null");
            if (name.trim().isEmpty()) {
                throw new IllegalArgumentException("name must not be empty");
            }
            bindings.put(name, binding);
            return this;
        }

        public final String render() {
            final StringBuilder sb = new StringBuilder();
            //FIXME we need better solution instead of instanceof
            for (final TemplatePart templatePart : templateParts) {
                if (templatePart instanceof TextTemplatePart) {
                    sb.append(((TextTemplatePart) templatePart).text());
                } else if (templatePart instanceof VariableTemplatePart) {
                    final VariableTemplatePart variableTemplatePart = (VariableTemplatePart) templatePart;
                    final Function<VariableTemplatePart.Parameter, String> binding =
                        bindings.get(variableTemplatePart.variableName());
                    // Substitute for bound variables, pass the variable pattern as is for non-bound.
                    if (Objects.nonNull(binding)) {
                        sb.append(binding.apply(variableTemplatePart.parameter()));
                    } else {
                        sb.append(variableTemplatePart.originalPlaceholder());
                    }
                }
            }
            return sb.toString();
        }
    }
}
