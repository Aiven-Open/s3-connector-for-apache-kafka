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

package io.aiven.kafka.connect.common.grouper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.templating.Template;

public final class KeyRecordGrouper implements RecordGrouper {

    private final Template filenameTemplate;

    // One record per file, but use List here for the compatibility with the interface.
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
        String recordKey = "";
        if (record.key() == null) {
            recordKey = "null";
        } else if (record.keySchema().type() == Schema.Type.STRING) {
            recordKey = (String) record.key();
        } else {
            recordKey = record.key().toString();
        }
        return filenameTemplate.render(null, "", 0, 0, recordKey);
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
