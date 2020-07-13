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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

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
