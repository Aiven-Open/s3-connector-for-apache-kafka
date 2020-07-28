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
        for (final OutputFieldType oft : OutputFieldType.values()) {
            if (oft.name.equalsIgnoreCase(name)) {
                return oft;
            }
        }
        throw new IllegalArgumentException("Unknown output field: " + name);
    }

    public static boolean isValidName(final String name) {
        return names().contains(name.toLowerCase());
    }

    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }
}
