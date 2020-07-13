package io.aiven.kafka.connect.common.config;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

public enum Variables {

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
    );

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

    public final String name;

    public final ParameterDescriptor parameterDescriptor;

    private Variables(final String name) {
        this(name, ParameterDescriptor.NO_PARAMETER);
    }

    private Variables(final String name,
                      final ParameterDescriptor parameterDescriptor) {
        this.name = name;
        this.parameterDescriptor = parameterDescriptor;
    }

    public static Variables of(final String name) {
        for (final Variables v : Variables.values()) {
            if (v.name.equals(name)) {
                return v;
            }
        }
        throw new IllegalArgumentException(
                String.format("Unknown variable: %s", name)
        );
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
