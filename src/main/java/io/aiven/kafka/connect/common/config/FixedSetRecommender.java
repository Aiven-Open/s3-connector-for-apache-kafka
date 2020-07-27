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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;

/**
 * A {@link ConfigDef.Recommender} that always supports only
 * the predefined set of values. {@link #visible(String, Map)} is always {@code true}.
 */
public class FixedSetRecommender implements ConfigDef.Recommender {

    private final List<Object> supportedValues;

    private FixedSetRecommender(final Collection<?> supportedValues) {
        Objects.requireNonNull(supportedValues, "supportedValues cannot be null");
        this.supportedValues = new ArrayList<>(supportedValues);
    }

    public static FixedSetRecommender ofSupportedValues(final Collection<?> supportedValues) {
        Objects.requireNonNull(supportedValues, "supportedValues cannot be null");
        return new FixedSetRecommender(supportedValues);
    }

    @Override
    public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
        return Collections.unmodifiableList(supportedValues);
    }

    @Override
    public boolean visible(final String name, final Map<String, Object> parsedConfig) {
        return true;
    }
}
