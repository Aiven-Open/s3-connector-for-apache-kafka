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

package io.aiven.kafka.connect.s3;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.common.templating.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3SinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(AivenKafkaConnectS3SinkConnector.class);

    private S3SinkConfig config;

    private S3StreamWriter streamMap;

    // required by Connect
    public S3SinkTask() {

    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props hasn't been set");
        this.config = new S3SinkConfig(props);
        this.streamMap = new S3StreamWriter(config);
    }

    @Override
    public void put(final Collection<SinkRecord> records) throws ConnectException {
        LOGGER.info("Processing {} records", records.size());
        records.stream()
            .map(r -> Pair.of(new TopicPartition(r.topic(), r.kafkaPartition()), r))
            .forEach(p -> streamMap.write(p.left(), p.right()));
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.keySet().forEach(streamMap::flush);
    }

    @Override
    public void close(final Collection<TopicPartition> partitions) throws ConnectException {
        partitions.forEach(tp -> {
            LOGGER.info("Close {}", tp);
            streamMap.close(tp);
        });
    }

    @Override
    public void stop() {
        LOGGER.info("Stop  S3 Sink Task");
        streamMap.closeAll();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

}
