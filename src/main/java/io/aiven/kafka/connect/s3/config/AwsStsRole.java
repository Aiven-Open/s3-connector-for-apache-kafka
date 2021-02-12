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

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;

final class AwsStsRole {

    // AssumeRole request limit details here:
    // https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
    public static final int MIN_SESSION_DURATION = STSAssumeRoleSessionCredentialsProvider.DEFAULT_DURATION_SECONDS;
    public static final int MAX_SESSION_DURATION = 43200;

    private final String arn;
    private final String externalId;
    private final String sessionName;
    private final int sessionDurationSeconds;

    public AwsStsRole(final String arn,
                      final String externalId,
                      final String sessionName,
                      final int sessionDurationSeconds) {
        this.arn = arn;
        this.externalId = externalId;
        this.sessionName = sessionName;
        this.sessionDurationSeconds = sessionDurationSeconds;
    }

    public String getArn() {
        return arn;
    }

    public String getExternalId() {
        return externalId;
    }

    public String getSessionName() {
        return sessionName;
    }

    public int getSessionDurationSeconds() {
        return sessionDurationSeconds;
    }

    public Boolean isValid() {
        return Objects.nonNull(arn) && Objects.nonNull(sessionName);
    }
}
