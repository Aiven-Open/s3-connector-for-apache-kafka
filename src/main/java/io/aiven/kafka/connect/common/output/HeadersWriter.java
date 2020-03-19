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

package io.aiven.kafka.connect.common.output;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

public class HeadersWriter implements OutputFieldWriter {
    private static final byte[] HEADER_KEY_VALUE_SEPARATOR = ":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEADERS_SEPARATOR = ";".getBytes(StandardCharsets.UTF_8);
    private final ByteArrayConverter byteArrayConverter = new ByteArrayConverter();

    @Override
    public void write(final SinkRecord record, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        Objects.requireNonNull(outputStream, "outputStream cannot be null");

        for (final Header header : record.headers()) {
            final String topic = record.topic();
            final String key = header.key();
            final Object value = header.value();
            final Schema schema = header.schema();
            outputStream.write(Base64.getEncoder().encode(key.getBytes()));
            outputStream.write(HEADER_KEY_VALUE_SEPARATOR);
            final byte[] bytes = byteArrayConverter.fromConnectHeader(topic, key, schema, value);
            outputStream.write(Base64.getEncoder().encode(bytes));
            outputStream.write(HEADERS_SEPARATOR);
        }
    }
}
