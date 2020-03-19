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

package io.aiven.kafka.connect.common.templating;

import java.util.Objects;

public final class TemplateParameter {

    public static final TemplateParameter EMPTY = new TemplateParameter("__EMPTY__", "__NO_VALUE__");

    private final String name;

    private final String value;

    private TemplateParameter(final String name, final String value) {
        this.name = name;
        this.value = value;
    }

    public static TemplateParameter of(final String name, final String value) {
        if (Objects.isNull(name) && Objects.isNull(value)) {
            return TemplateParameter.EMPTY;
        } else {
            Objects.requireNonNull(name, "name has not been set");
            Objects.requireNonNull(value, "value has not been set");
            return new TemplateParameter(name, value);
        }
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    public String name() {
        return name;
    }

    public String value() {
        return value;
    }

    public final Boolean asBoolean() {
        return Boolean.parseBoolean(value);
    }
}
