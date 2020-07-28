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
