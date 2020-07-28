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
