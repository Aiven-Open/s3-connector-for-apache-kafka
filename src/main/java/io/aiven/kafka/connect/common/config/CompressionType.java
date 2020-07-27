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

    public static final String SUPPORTED_COMPRESSION_TYPES =
        CompressionType.names().stream()
            .map(c -> String.format("'%s'", c))
            .collect(Collectors.joining(", "));

    public final String name;

    CompressionType(final String name) {
        this.name = name;
    }

    public static CompressionType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        for (final CompressionType ct : CompressionType.values()) {
            if (ct.name.equalsIgnoreCase(name)) {
                return ct;
            }
        }
        throw new IllegalArgumentException("Unknown compression type: " + name);
    }

    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }

    public abstract String extension();
}
