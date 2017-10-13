/*
 * Copyright 2017 Expedia, Inc.
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
