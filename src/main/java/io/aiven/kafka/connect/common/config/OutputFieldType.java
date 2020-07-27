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
