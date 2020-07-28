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
