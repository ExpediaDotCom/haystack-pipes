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
package com.expedia.www.haystack.pipes.kafka;

import com.expedia.www.haystack.commons.config.Configuration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.cfg4j.provider.ConfigurationProvider;

import java.util.HashMap;
import java.util.Map;

import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_KAFKA_PRODUCER_CONFIG_PREFIX;

public class KafkaProducerConfigurationProvider implements KafkaProducerConfig {
    private KafkaProducerConfig kafkaProducerConfig;

    KafkaProducerConfigurationProvider() {
        reload();
    }

    @Override
    public String brokers() {
        return kafkaProducerConfig.brokers();
    }

    @Override
    public int port() {
        return kafkaProducerConfig.port();
    }

    @Override
    public String totopic() {
        return kafkaProducerConfig.totopic();
    }

    @Override
    public String acks() {
        return kafkaProducerConfig.acks();
    }

    @Override
    public int batchsize() {
        return kafkaProducerConfig.batchsize();
    }

    @Override
    public int lingerms() {
        return kafkaProducerConfig.lingerms();
    }

    @Override
    public int buffermemory() {
        return kafkaProducerConfig.buffermemory();
    }

    void reload() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
        kafkaProducerConfig = configurationProvider.bind(HAYSTACK_KAFKA_PRODUCER_CONFIG_PREFIX, KafkaProducerConfig.class);
    }

    Map<String, Object> getConfigurationMap() {
        final Map<String, Object> map = new HashMap<>();
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers());
        map.put(ProducerConfig.ACKS_CONFIG, acks());
        map.put(ProducerConfig.RETRIES_CONFIG, 3);
        map.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        map.put(ProducerConfig.BATCH_SIZE_CONFIG, batchsize());
        map.put(ProducerConfig.LINGER_MS_CONFIG, lingerms());
        map.put(ProducerConfig.BUFFER_MEMORY_CONFIG, buffermemory());
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return map;
    }


}
