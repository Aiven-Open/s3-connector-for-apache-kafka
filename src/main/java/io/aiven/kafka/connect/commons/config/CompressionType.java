package io.aiven.kafka.connect.commons.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public enum CompressionType {
    NONE("none") {
        @Override
        public String extension() {
            return "";
        }
    },
    GZIP("gzip") {
        @Override
        public String extension() {
            return ".gz";
        }
    },
    SNAPPY("snappy") {
        @Override
        public String extension() {
            return ".snappy";
        }
    },
    ZSTD("zstd") {
        @Override
        public String extension() {
            return ".zst";
        }
    };

    public final String name;

    CompressionType(final String name) {
        this.name = name;

    }

    public static CompressionType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");

        if (NONE.name.equalsIgnoreCase(name)) {
            return NONE;
        }
        if (GZIP.name.equalsIgnoreCase(name)) {
            return GZIP;
        }
        if (SNAPPY.name.equalsIgnoreCase(name)) {
            return SNAPPY;
        }
        if (ZSTD.name.equalsIgnoreCase(name)) {
            return ZSTD;
        }
        throw new IllegalArgumentException("Unknown compression type: " + name);
    }

    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }

    public abstract String extension();
}
