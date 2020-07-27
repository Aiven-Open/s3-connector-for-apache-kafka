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

package io.aiven.kafka.connect.common.config;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.templating.Pair;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

import com.google.common.collect.ImmutableMap;

import static io.aiven.kafka.connect.common.grouper.RecordGrouperFactory.ALL_SUPPORTED_VARIABLES;
import static io.aiven.kafka.connect.common.grouper.RecordGrouperFactory.SUPPORTED_VARIABLES_LIST;

public final class FilenameTemplateValidator implements ConfigDef.Validator {

    private static final Map<String, FilenameTemplateVariable.ParameterDescriptor> SUPPORTED_VARIABLE_PARAMETERS =
        ImmutableMap.of(
            FilenameTemplateVariable.START_OFFSET.name,
            FilenameTemplateVariable.START_OFFSET.parameterDescriptor,

            FilenameTemplateVariable.TIMESTAMP.name,
            FilenameTemplateVariable.TIMESTAMP.parameterDescriptor
        );

    private final String configName;

    public FilenameTemplateValidator(final String configName) {
        this.configName = configName;
    }

    private static void validateVariables(final Set<String> variables) {
        for (final String variable : variables) {
            if (!ALL_SUPPORTED_VARIABLES.contains(variable)) {
                throw new IllegalArgumentException(
                    String.format(
                        "unsupported set of template variables, supported sets are: %s",
                        SUPPORTED_VARIABLES_LIST
                    )
                );
            }
        }
    }

    public static void validateVariablesWithRequiredParameters(
        final List<Pair<String, Parameter>> variablesWithParameters) {
        for (final Pair<String, Parameter> p : variablesWithParameters) {
            final String varName = p.left();
            final Parameter varParam = p.right();
            if (SUPPORTED_VARIABLE_PARAMETERS.containsKey(varName)) {
                final FilenameTemplateVariable.ParameterDescriptor expectedParameter =
                    SUPPORTED_VARIABLE_PARAMETERS.get(varName);
                if (varParam.isEmpty() && expectedParameter.required) {
                    throw new IllegalArgumentException(
                        String.format(
                            "parameter %s is required for the the variable %s, supported values are: %s",
                            expectedParameter.name, varName, expectedParameter.toString()
                        )
                    );
                }
            }
        }
    }

    @Override
    public final void ensureValid(final String name, final Object value) {
        if (value == null) {
            return;
        }

        assert value instanceof String;

        // See https://cloud.google.com/storage/docs/naming
        final String valueStr = (String) value;
        if (valueStr.startsWith(".well-known/acme-challenge")) {
            throw new ConfigException(configName, value,
                "cannot start with '.well-known/acme-challenge'");
        }

        try {
            final Template template = Template.of((String) value);
            validateVariables(template.variablesSet());
            validateVariableParameters(template.variablesWithNonEmptyParameters());
            validateVariablesWithRequiredParameters(template.variablesWithParameters());
            RecordGrouperFactory.resolveRecordGrouperType(template);
        } catch (final IllegalArgumentException e) {
            throw new ConfigException(configName, value, e.getMessage());
        }


    }

    public void validateVariableParameters(final List<Pair<String, Parameter>> variablesWithNonEmptyParameters) {
        boolean isVariableParametersSupported = true;
        for (final Pair<String, Parameter> e : variablesWithNonEmptyParameters) {
            final String varName = e.left();
            final Parameter varParam = e.right();
            if (SUPPORTED_VARIABLE_PARAMETERS.containsKey(varName)) {
                final FilenameTemplateVariable.ParameterDescriptor expectedParameter =
                    SUPPORTED_VARIABLE_PARAMETERS.get(varName);
                if (!expectedParameter.values.contains(varParam.value())) {
                    isVariableParametersSupported = false;
                    break;
                }
            }
        }
        if (!isVariableParametersSupported) {
            final String supportedParametersSet = SUPPORTED_VARIABLE_PARAMETERS.keySet().stream()
                .map(v -> FilenameTemplateVariable.of(v).description())
                .collect(Collectors.joining(","));
            throw new IllegalArgumentException(
                String.format(
                    "unsupported set of template variables parameters, supported sets are: %s",
                    supportedParametersSet
                )
            );
        }
    }
}
