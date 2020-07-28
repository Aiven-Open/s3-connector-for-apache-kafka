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
