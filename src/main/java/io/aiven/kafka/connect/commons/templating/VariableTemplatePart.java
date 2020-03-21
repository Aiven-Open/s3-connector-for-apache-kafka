package io.aiven.kafka.connect.commons.templating;

import java.util.Objects;

public class VariableTemplatePart implements TemplatePart {

    private final String variableName;

    private final Parameter parameter;

    private final String originalPlaceholder;

    protected VariableTemplatePart(
            final String variableName,
            final String originalPlaceholder) {
        this(variableName, Parameter.EMPTY, originalPlaceholder);
    }

    protected VariableTemplatePart(
            final String variableName,
            final Parameter parameter,
            final String originalPlaceholder) {
        this.variableName = variableName;
        this.parameter = parameter;
        this.originalPlaceholder = originalPlaceholder;
    }

    public final String variableName() {
        return variableName;
    }

    public final Parameter parameter() {
        return parameter;
    }

    public final String originalPlaceholder() {
        return originalPlaceholder;
    }

    public static final class Parameter {

        public static final Parameter EMPTY =
                new Parameter("__EMPTY__", "__NO_VALUE__");

        private final String name;

        private final String value;

        private Parameter(final String name, final String value) {
            this.name = name;
            this.value = value;
        }

        public boolean isEmpty() {
            return this == EMPTY;
        }

        public String name() {
            return name;
        }

        public String value() {
            return value;
        }

        public final Boolean asBoolean() {
            return Boolean.parseBoolean(value);
        }

        public static Parameter of(final String name, final String value) {
            if (Objects.isNull(name) && Objects.isNull(value)) {
                return Parameter.EMPTY;
            } else {
                Objects.requireNonNull(name, "name has not been set");
                Objects.requireNonNull(value, "value has not been set");
                return new Parameter(name, value);
            }
        }

    }

}
