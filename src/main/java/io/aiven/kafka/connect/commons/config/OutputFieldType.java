package io.aiven.kafka.connect.commons.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public enum OutputFieldType {
    KEY("key"),
    VALUE("value"),
    OFFSET("offset"),
    TIMESTAMP("timestamp"),
    HEADERS("headers");

    public final String name;

    OutputFieldType(final String name) {
        this.name = name;
    }

    public static OutputFieldType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");

        if (KEY.name.equalsIgnoreCase(name)) {
            return KEY;
        } else if (VALUE.name.equalsIgnoreCase(name)) {
            return VALUE;
        } else if (OFFSET.name.equalsIgnoreCase(name)) {
            return OFFSET;
        } else if (TIMESTAMP.name.equalsIgnoreCase(name)) {
            return TIMESTAMP;
        } else if (HEADERS.name.equalsIgnoreCase(name)) {
            return HEADERS;
        } else {
            throw new IllegalArgumentException("Unknown output field: " + name);
        }
    }

    public static boolean isValidName(final String name) {
        return names().contains(name.toLowerCase());
    }

    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }
}
