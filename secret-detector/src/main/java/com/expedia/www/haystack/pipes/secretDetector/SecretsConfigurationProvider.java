package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.www.haystack.pipes.commons.Configuration;
import org.cfg4j.provider.ConfigurationProvider;

import java.util.List;

public class SecretsConfigurationProvider implements SecretsConfig {
    private static final String HAYSTACK_SECRETS_CONFIG_PREFIX = "haystack.secretsnotifications";

    private final SecretsConfig secretsConfig;

    SecretsConfigurationProvider() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        secretsConfig = configurationProvider.bind(HAYSTACK_SECRETS_CONFIG_PREFIX, SecretsConfig.class);
    }

    @Override
    public List<String> emails() {
        return secretsConfig.emails();
    }
}
