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

package io.aiven.kafka.connect.s3.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.junit.jupiter.api.Test;

import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_ACCESS_KEY_ID;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_SECRET_ACCESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3SinkCredentialsConfigTest {
    @Test
    final void emptyAwsAccessKeyID() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [hidden] for configuration aws_access_key_id: Password must be non-empty");

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [hidden] for configuration aws.access.key.id: Password must be non-empty");
    }

    @Test
    final void emptyAwsSecretAccessKey() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "");

        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [hidden] for configuration aws_secret_access_key: Password must be non-empty");

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "");
        assertThatThrownBy(() -> new S3SinkConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value [hidden] for configuration aws.secret.access.key: Password must be non-empty");
    }

    /**
     * Even when no sts role or session name is provided we should be able to create a configuration since it will
     * fall back to using default credentials.
     */
    @Test
    final void defaultCredentials() {
        final Map<String, String> props = Map.of(AWS_S3_BUCKET_NAME_CONFIG, "test-bucket");
        final S3SinkConfig config = new S3SinkConfig(props);
        assertThat(config.getAwsCredentials().isValid()).isFalse();
        assertThat(config.getCustomCredentialsProvider()).isInstanceOf(DefaultAWSCredentialsProviderChain.class);
    }
}
