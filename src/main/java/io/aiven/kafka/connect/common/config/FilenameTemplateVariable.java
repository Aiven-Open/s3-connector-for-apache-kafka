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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

public enum FilenameTemplateVariable {
    TOPIC("topic"),
    PARTITION("partition"),
    START_OFFSET(
        "start_offset",
        new ParameterDescriptor(
            "padding",
            false,
            ImmutableSet.of(Boolean.TRUE.toString(), Boolean.FALSE.toString())
        )
    ),
    TIMESTAMP(
        "timestamp",
        new ParameterDescriptor(
            "unit",
            true,
            ImmutableSet.of("YYYY", "MM", "dd", "HH")
        )
    ),
    KEY("key");

    public final String name;

    public final ParameterDescriptor parameterDescriptor;

    FilenameTemplateVariable(final String name) {
        this(name, ParameterDescriptor.NO_PARAMETER);
    }

    FilenameTemplateVariable(final String name,
                             final ParameterDescriptor parameterDescriptor) {
        this.name = name;
        this.parameterDescriptor = parameterDescriptor;
    }

    public static FilenameTemplateVariable of(final String name) {
        for (final FilenameTemplateVariable v : FilenameTemplateVariable.values()) {
            if (v.name.equals(name)) {
                return v;
            }
        }
        throw new IllegalArgumentException(
            String.format(
                "Unknown filename template variable: %s",
                name)
        );
    }

    public String description() {
        return
            (parameterDescriptor != ParameterDescriptor.NO_PARAMETER && !parameterDescriptor.values.isEmpty())
                ? String.join(
                "=",
                String.join(
                    ":",
                    name,
                    parameterDescriptor.name),
                parameterDescriptor.toString()
            ) : name;
    }

    public static class ParameterDescriptor {

        public static final ParameterDescriptor NO_PARAMETER =
            new ParameterDescriptor(
                "__no_parameter__",
                false,
                Collections.emptySet());

        public final String name;

        public final boolean required;

        public Set<String> values;

        public ParameterDescriptor(final String name,
                                   final boolean required,
                                   final Set<String> values) {
            this.name = name;
            this.required = required;
            this.values = values;
        }

        @Override
        public String toString() {
            return !values.isEmpty()
                ? String.join("|", values)
                : "";
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ParameterDescriptor)) {
                return false;
            }
            final ParameterDescriptor that = (ParameterDescriptor) o;
            return required == that.required
                && Objects.equals(name, that.name)
                && Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, required, values);
        }
    }

}
