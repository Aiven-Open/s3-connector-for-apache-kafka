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

import java.util.stream.Collectors;

import com.google.common.base.Objects;

public class OutputField {
    public static final String SUPPORTED_OUTPUT_FIELDS =
        OutputFieldType.names().stream()
            .map(f -> "'" + f + "'")
            .collect(Collectors.joining(", "));

    private final OutputFieldType fieldType;
    private final OutputFieldEncodingType encodingType;

    public OutputField(final OutputFieldType fieldType, final OutputFieldEncodingType encodingType) {
        this.fieldType = fieldType;
        this.encodingType = encodingType;
    }

    public OutputFieldType getFieldType() {
        return fieldType;
    }

    public OutputFieldEncodingType getEncodingType() {
        return encodingType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldType, encodingType);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof OutputField)) {
            return false;
        }

        final OutputField that = (OutputField) obj;

        return Objects.equal(this.fieldType, that.fieldType)
            && Objects.equal(this.encodingType, that.encodingType);
    }
}
