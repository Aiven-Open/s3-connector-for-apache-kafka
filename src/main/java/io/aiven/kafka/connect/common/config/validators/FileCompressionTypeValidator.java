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

package io.aiven.kafka.connect.common.config.validators;

import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.CompressionType;

public class FileCompressionTypeValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(final String name, final Object value) {
        // is it up to the connector decide how to support default values for compression.
        // The reason is that for different connectors there is the different compression type
        if (Objects.nonNull(value)) {
            final String valueStr = (String) value;
            if (!CompressionType.names().contains(valueStr)) {
                throw new ConfigException(
                    name, valueStr,
                    "supported values are: " + CompressionType.SUPPORTED_COMPRESSION_TYPES);
            }
        }
    }
}
