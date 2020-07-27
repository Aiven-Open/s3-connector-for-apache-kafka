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

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public interface TimestampSource {

    static TimestampSource of(final Type extractorType) {
        return of(ZoneOffset.UTC, extractorType);
    }

    static TimestampSource of(final ZoneId zoneId, final Type extractorType) {
        if (extractorType == Type.WALLCLOCK) {
            return new WallclockTimestampSource(zoneId);
        }
        throw new IllegalArgumentException(
            String.format("Unsupported timestamp extractor type: %s", extractorType)
        );
    }

    ZonedDateTime time();

    enum Type {
        WALLCLOCK;

        public static Type of(final String name) {
            for (final Type t : Type.values()) {
                if (t.name().equalsIgnoreCase(name)) {
                    return t;
                }
            }
            throw new IllegalArgumentException(String.format("Unknown timestamp source: %s", name));
        }
    }

    final class WallclockTimestampSource implements TimestampSource {
        private final ZoneId zoneId;

        protected WallclockTimestampSource(final ZoneId zoneId) {
            this.zoneId = zoneId;
        }

        @Override
        public ZonedDateTime time() {
            return ZonedDateTime.now(zoneId);
        }
    }
}
