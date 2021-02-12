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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

public class AwsCredentialProviderFactory {
    public AWSCredentialsProvider getProvider(final S3SinkConfig config) {
        if (config.hasAwsStsRole()) {
            return getStsProvider(config);
        }
        return getBasicAwsCredentialsProvider(config);
    }

    private AWSCredentialsProvider getStsProvider(final S3SinkConfig config) {
        final AwsStsRole awsstsRole = config.getStsRole();
        final AWSSecurityTokenService sts = securityTokenService(config);
        return new STSAssumeRoleSessionCredentialsProvider.Builder(awsstsRole.getArn(), awsstsRole.getSessionName())
                .withStsClient(sts)
                .withExternalId(awsstsRole.getExternalId())
                .withRoleSessionDurationSeconds(awsstsRole.getSessionDurationSeconds())
                .build();
    }

    private AWSSecurityTokenService securityTokenService(final S3SinkConfig config) {
        if (config.hasStsEndpointConfig()) {
            final AwsStsEndpointConfig endpointConfig = config.getStsEndpointConfig();
            final AwsClientBuilder.EndpointConfiguration stsConfig =
                    new AwsClientBuilder.EndpointConfiguration(endpointConfig.getServiceEndpoint(),
                                                               endpointConfig.getSigningRegion());
            final AWSSecurityTokenServiceClientBuilder stsBuilder =
                    AWSSecurityTokenServiceClientBuilder.standard();
            stsBuilder.setEndpointConfiguration(stsConfig);
            return stsBuilder.build();
        }
        return AWSSecurityTokenServiceClientBuilder.defaultClient();
    }

    private AWSCredentialsProvider getBasicAwsCredentialsProvider(final S3SinkConfig config) {
        final AwsAccessSecret awsCredentials = config.getAwsCredentials();
        return new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(
                        awsCredentials.getAccessKeyId().value(),
                        awsCredentials.getSecretAccessKey().value()
                )
        );
    }

}
