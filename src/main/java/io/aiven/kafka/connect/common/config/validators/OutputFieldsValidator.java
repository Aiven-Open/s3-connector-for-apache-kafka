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

package io.aiven.kafka.connect.common.config.validators;

import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldType;

public class OutputFieldsValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(final String name, final Object value) {
        if (Objects.nonNull(value)) {
            @SuppressWarnings("unchecked") final List<String> valueList = (List<String>) value;
            if (valueList.isEmpty()) {
                throw new ConfigException(name, valueList, "cannot be empty");
            }
            for (final String fieldName : valueList) {
                if (!OutputFieldType.isValidName(fieldName)) {
                    throw new ConfigException(
                        name, value,
                        "supported values are: " + OutputField.SUPPORTED_OUTPUT_FIELDS);
                }
            }
        }
    }

}
