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

package io.aiven.kafka.connect.s3;

import java.util.HashMap;

import io.aiven.kafka.connect.s3.config.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.config.S3SinkConfig;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Regions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AwsCredentialProviderFactoryTest {

    private AwsCredentialProviderFactory factory;
    private HashMap<String, String> props;

    @BeforeEach
    public void setUp() {
        factory = new AwsCredentialProviderFactory();
        props = new HashMap<>();
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "anyBucket");
    }

    @Test
    void createsStsCredentialProviderIfSpecified() {
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());
        props.put(S3SinkConfig.AWS_STS_CONFIG_ENDPOINT, "https://sts.us-east-1.amazonaws.com");

        final var config = new S3SinkConfig(props);

        final var credentialProvider = factory.getProvider(config);
        assertThat(credentialProvider).isInstanceOf(STSAssumeRoleSessionCredentialsProvider.class);
    }

    @Test
    void createStaticCredentialProviderByDefault() {
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");

        final var config = new S3SinkConfig(props);

        final var credentialProvider = factory.getProvider(config);
        assertThat(credentialProvider).isInstanceOf(AWSStaticCredentialsProvider.class);
    }
}
