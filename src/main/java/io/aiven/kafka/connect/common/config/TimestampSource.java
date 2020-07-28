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
