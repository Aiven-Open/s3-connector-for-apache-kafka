package io.aiven.kafka.connect.commons.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public enum OutputFieldEncodingType {
    NONE("none"),
    BASE64("base64");

    public final String name;

    OutputFieldEncodingType(final String name) {
        this.name = name;
    }

    public static OutputFieldEncodingType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");

        if (NONE.name.equalsIgnoreCase(name)) {
            return NONE;
        } else if (BASE64.name.equalsIgnoreCase(name)) {
            return BASE64;
        } else {
            throw new IllegalArgumentException("Unknown output field encoding type: " + name);
        }
    }

    public static boolean isValidName(final String name) {
        return names().contains(name.toLowerCase());
    }

    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }
}
