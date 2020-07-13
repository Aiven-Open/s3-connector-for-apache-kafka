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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;

public class OutputFieldsEncodingValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(final String name, final Object value) {
        assert value instanceof String;
        final String valueStr = (String) value;
        if (!OutputFieldEncodingType.names().contains(valueStr)) {
            throw new ConfigException(
                name,
                valueStr,
                "supported values are: " + OutputFieldEncodingType.SUPPORTED_FIELD_ENCODING_TYPES);
        }
    }

}
