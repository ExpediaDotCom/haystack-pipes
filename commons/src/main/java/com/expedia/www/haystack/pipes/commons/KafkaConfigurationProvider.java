package com.expedia.www.haystack.pipes.commons;

import org.cfg4j.provider.ConfigurationProvider;

import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_KAFKA_CONFIG_PREFIX;

public class KafkaConfigurationProvider implements KafkaConfig {
    private final KafkaConfig kafkaConfig;

    public KafkaConfigurationProvider() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        kafkaConfig = configurationProvider.bind(HAYSTACK_KAFKA_CONFIG_PREFIX, KafkaConfig.class);
    }

    @Override
    public String brokers() {
        return kafkaConfig.brokers();
    }

    @Override
    public int port() {
        return kafkaConfig.port();
    }

    @Override
    public String fromTopic() {
        return kafkaConfig.fromTopic();
    }

    @Override
    public String toTopic() {
        return kafkaConfig.toTopic();
    }
}
