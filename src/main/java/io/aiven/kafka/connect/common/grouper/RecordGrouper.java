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

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * The interface for classes that associates {@link SinkRecord}s with files by some criteria.
 */
public interface RecordGrouper {
    /**
     * Associate the record with the appropriate file.
     */
    void put(SinkRecord record);

    /**
     * Clear all records.
     */
    void clear();

    /**
     * Get all records associated with files, grouped by the file name.
     */
    Map<String, List<SinkRecord>> records();

}
