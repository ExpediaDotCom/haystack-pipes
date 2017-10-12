package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.www.haystack.pipes.commons.Configuration;
import org.cfg4j.provider.ConfigurationProvider;

import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_EXTERNAL_KAFKA_CONFIG_PREFIX;

public class ExternalKafkaConfigurationProvider implements ExternalKafkaConfig {
    private final ExternalKafkaConfig externalKafkaConfig;

    ExternalKafkaConfigurationProvider() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        externalKafkaConfig = configurationProvider.bind(HAYSTACK_EXTERNAL_KAFKA_CONFIG_PREFIX, ExternalKafkaConfig.class);
    }

    @Override
    public String brokers() {
        return externalKafkaConfig.brokers();
    }

    @Override
    public int port() {
        return externalKafkaConfig.port();
    }

    @Override
    public String toTopic() {
        return externalKafkaConfig.toTopic();
    }

    @Override
    public String acks() {
        return externalKafkaConfig.acks();
    }

    @Override
    public int batchSize() {
        return externalKafkaConfig.batchSize();
    }

    @Override
    public int lingerMs() {
        return externalKafkaConfig.lingerMs();
    }

    @Override
    public int bufferMemory() {
        return externalKafkaConfig.bufferMemory();
    }
}
