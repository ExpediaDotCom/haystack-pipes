/*
 * Copyright 2018 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.secretDetector.config;

import com.expedia.www.haystack.pipes.commons.Configuration;
import org.cfg4j.provider.ConfigurationProvider;

import java.util.List;

public class SecretsEmailConfigurationProvider implements SecretsEmailConfig {
    private static final String HAYSTACK_SECRETS_CONFIG_PREFIX = "haystack.secretsnotifications.email";

    private final SecretsEmailConfig secretsEmailConfig;

    public SecretsEmailConfigurationProvider() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        secretsEmailConfig = configurationProvider.bind(HAYSTACK_SECRETS_CONFIG_PREFIX, SecretsEmailConfig.class);
    }

    @Override
    public String from() {
        return secretsEmailConfig.from();
    }

    @Override
    public List<String> tos() {
        return secretsEmailConfig.tos();
    }

    @Override
    public String host() {
        return secretsEmailConfig.host();
    }

    @Override
    public String subject() {
        return secretsEmailConfig.subject();
    }
}
