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
