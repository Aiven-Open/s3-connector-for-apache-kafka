package io.aiven.kafka.connect.commons.config;

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;

public interface OutputFieldWriter {

    void write(SinkRecord record, OutputStream outputStream) throws IOException;

}
