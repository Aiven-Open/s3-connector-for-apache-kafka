/*
 * Copyright 2021 Aiven Oy
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

import java.util.Objects;

import org.apache.kafka.common.config.types.Password;

final class AwsAccessSecret {
    private final Password accessKeyId;
    private final Password secretAccessKey;

    public AwsAccessSecret(final Password accessKeyId, final Password secretAccessKey) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
    }

    public Password getAccessKeyId() {
        return accessKeyId;
    }

    public Password getSecretAccessKey() {
        return secretAccessKey;
    }

    public Boolean isValid() {
        return Objects.nonNull(accessKeyId) && Objects.nonNull(secretAccessKey);
    }
}
