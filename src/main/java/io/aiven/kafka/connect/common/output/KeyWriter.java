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

package io.aiven.kafka.connect.common.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public final class KeyWriter implements OutputFieldWriter {
    /**
     * Takes the {@link SinkRecord}'s key as a byte array.
     *
     * <p>If the key is {@code null}, it outputs nothing.
     *
     * <p>If the key is not {@code null}, it assumes the key <b>is</b> a byte array.
     *
     * @param record       the record to get the key from
     * @param outputStream the stream to write to
     * @throws DataException when the key is not actually a byte array
     */
    @Override
    public void write(final SinkRecord record,
                      final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(record.keySchema(), "key schema cannot be null");
        Objects.requireNonNull(outputStream, "outputStream cannot be null");

        if (record.keySchema().type() != Schema.Type.BYTES
            && record.keySchema().type() != Schema.Type.STRING) {
            final String msg = String.format("Record key schema type must be %s or %s, %s given",
                Schema.Type.BYTES, Schema.Type.STRING, record.keySchema().type());
            throw new DataException(msg);
        }

        // Do nothing if the key is null.
        if (record.key() == null) {
            return;
        }

        if (record.key() instanceof byte[]) {
            outputStream.write(Base64.getEncoder().encode((byte[]) record.key()));
        } else if (record.key() instanceof String) {
            outputStream.write(Base64.getEncoder().encode(((String) record.key()).getBytes()));
        } else {
            throw new DataException("Key is not byte[] or String");
        }
    }
}
