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

package io.aiven.kafka.connect.common.grouper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.templating.Template;

public final class KeyRecordGrouper implements RecordGrouper {

    private final Template filenameTemplate;

    // One record pre file, but use List here for the compatibility with the interface.
    private final Map<String, List<SinkRecord>> fileBuffers = new HashMap<>();

    /**
     * A constructor.
     *
     * @param filenameTemplate the filename template.
     */
    public KeyRecordGrouper(final Template filenameTemplate) {
        Objects.requireNonNull(filenameTemplate, "filenameTemplate cannot be null");
        this.filenameTemplate = filenameTemplate;
    }

    @Override
    public void put(final SinkRecord record) {
        Objects.requireNonNull(record, "records cannot be null");

        final String recordKey = generateRecordKey(record);

        fileBuffers.putIfAbsent(recordKey, new ArrayList<>());

        // one record per file
        final List<SinkRecord> records = fileBuffers.get(recordKey);
        records.clear();
        records.add(record);
    }

    private String generateRecordKey(final SinkRecord record) {
        final Supplier<String> setKey = () -> {
            if (record.key() == null) {
                return "null";
            } else if (record.keySchema().type() == Schema.Type.STRING) {
                return (String) record.key();
            } else {
                return record.key().toString();
            }
        };

        return filenameTemplate.instance()
            .bindVariable(FilenameTemplateVariable.KEY.name, setKey)
            .render();
    }

    @Override
    public void clear() {
        fileBuffers.clear();
    }

    @Override
    public Map<String, List<SinkRecord>> records() {
        return Collections.unmodifiableMap(fileBuffers);
    }

}
