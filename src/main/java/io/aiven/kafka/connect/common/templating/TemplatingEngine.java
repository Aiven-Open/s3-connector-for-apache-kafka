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
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A simple templating engine that allows to bind variables to supplier functions.
 * Variable syntax: {@code {{ variable_name }}} (arbitrary number of space inside the braces).
 * Non-bound variables are left as is.
 */
public final class TemplatingEngine {
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{\\{\\s*(\\w+)\\s*}}"); // {{ var }}

    private final Map<String, Supplier<String>> bindings = new HashMap<>();

    public final void bindVariable(final String name, final Supplier<String> supplier) {
        bindings.put(name, supplier);
    }

    public final String render(final String template) {
        final Matcher matcher = VARIABLE_PATTERN.matcher(template);
        final StringBuilder sb = new StringBuilder();

        int position = 0;
        while (matcher.find()) {
            sb.append(template, position, matcher.start());

            final String variableName = matcher.group(1);
            final Supplier<String> supplier = bindings.get(variableName);
            // Substitute for bound variables, pass the variable pattern as is for non-bound.
            if (supplier != null) {
                sb.append(supplier.get());
            } else {
                sb.append(matcher.group());
            }
            position = matcher.end();
        }
        sb.append(template.substring(position));

        return sb.toString();
    }
}
