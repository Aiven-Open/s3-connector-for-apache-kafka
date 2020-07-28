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
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public abstract class AbstractValueWriter implements OutputFieldWriter {
    /**
     * Takes the {@link SinkRecord}'s value as a byte array.
     *
     * <p>If the value is {@code null}, it outputs nothing.
     *
     * <p>If the value is not {@code null}, it assumes the value <b>is</b> a byte array.
     *
     * @param record       the record to get the value from
     * @param outputStream the stream to write to
     * @throws DataException when the value is not actually a byte array
     */
    @Override
    public void write(final SinkRecord record,
                      final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(record.valueSchema(), "value schema cannot be null");
        Objects.requireNonNull(outputStream, "outputStream cannot be null");

        if (record.valueSchema().type() != Schema.Type.BYTES) {
            final String msg = String.format("Record value schema type must be %s, %s given",
                Schema.Type.BYTES, record.valueSchema().type());
            throw new DataException(msg);
        }

        // Do nothing if the key is null.
        if (record.value() == null) {
            return;
        }

        if (!(record.value() instanceof byte[])) {
            throw new DataException("Value is not a byte array");
        }

        outputStream.write(getOutputBytes((byte[]) record.value()));
    }

    protected abstract byte[] getOutputBytes(final byte[] value);
}
