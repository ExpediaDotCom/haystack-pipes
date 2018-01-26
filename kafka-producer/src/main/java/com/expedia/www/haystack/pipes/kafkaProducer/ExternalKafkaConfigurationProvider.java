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
package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.www.haystack.pipes.commons.Configuration;
import org.cfg4j.provider.ConfigurationProvider;

import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_EXTERNAL_KAFKA_CONFIG_PREFIX;

public class ExternalKafkaConfigurationProvider implements ExternalKafkaConfig {
    private ExternalKafkaConfig externalKafkaConfig;

    ExternalKafkaConfigurationProvider() {
        reload();
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
    public String totopic() {
        return externalKafkaConfig.totopic();
    }

    @Override
    public String acks() {
        return externalKafkaConfig.acks();
    }

    @Override
    public int batchsize() {
        return externalKafkaConfig.batchsize();
    }

    @Override
    public int lingerms() {
        return externalKafkaConfig.lingerms();
    }

    @Override
    public int buffermemory() {
        return externalKafkaConfig.buffermemory();
    }

    void reload() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        externalKafkaConfig = configurationProvider.bind(HAYSTACK_EXTERNAL_KAFKA_CONFIG_PREFIX, ExternalKafkaConfig.class);
    }
}
