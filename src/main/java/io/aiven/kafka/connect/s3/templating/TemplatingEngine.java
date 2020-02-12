/*
 * Copyright 2020 Aiven Oy
 * Copyright 2018 Confluent Inc.
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

package io.aiven.kafka.connect.s3.templating;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A simple templating engine that allows to bind variables to supplier functions.
 * Variable syntax: {@code {{ variable_name }}} (arbitrary number of space inside the braces).
 * Non-bound variables are left as is.
 **/
public final class TemplatingEngine {
    private static Pattern variablePattern = Pattern.compile("\\{\\{\\s*(\\w+)\\s*}}"); // {{ var }}

    private final Map<String, Supplier<String>> bindings = new HashMap<>();

    public final void bindVariable(final String name, final Supplier<String> supplier) {
        bindings.put(name, supplier);
    }

    public final String render(final String template) {
        final Matcher m = variablePattern.matcher(template);
        final StringBuilder sb = new StringBuilder();

        int position = 0;
        while (m.find()) {
            sb.append(template, position, m.start());

            final String variableName = m.group(1);
            final Supplier<String> supplier = bindings.get(variableName);
            // Substitute for bound variables, pass the variable pattern as is for non-bound.
            if (supplier != null) {
                sb.append(supplier.get());
            } else {
                sb.append(m.group());
            }
            position = m.end();
        }
        sb.append(template.substring(position));

        return sb.toString();
    }
}
