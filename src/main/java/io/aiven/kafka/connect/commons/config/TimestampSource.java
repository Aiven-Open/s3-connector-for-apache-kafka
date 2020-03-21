package io.aiven.kafka.connect.commons.config;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public interface TimestampSource {

    ZonedDateTime time();

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
