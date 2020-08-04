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

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);

    private static final String PROPERTIES_FILENAME = "aiven-kafka-connect-s3-version.properties";

    static final String VERSION;

    static {
        final Properties props = new Properties();
        try (final InputStream resourceStream =
                 Version.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME)) {
            props.load(resourceStream);
        } catch (final Exception e) {
            log.warn("Error while loading {}: {}", PROPERTIES_FILENAME, e.getMessage());
        }
        VERSION = props.getProperty("version", "unknown").trim();
    }
}
