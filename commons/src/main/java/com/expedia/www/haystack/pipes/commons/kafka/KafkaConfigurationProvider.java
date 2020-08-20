///*
// * Copyright 2018 Expedia, Inc.
// *
// *       Licensed under the Apache License, Version 2.0 (the "License");
// *       you may not use this file except in compliance with the License.
// *       You may obtain a copy of the License at
// *
// *           http://www.apache.org/licenses/LICENSE-2.0
// *
// *       Unless required by applicable law or agreed to in writing, software
// *       distributed under the License is distributed on an "AS IS" BASIS,
// *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *       See the License for the specific language governing permissions and
// *       limitations under the License.
// *
// */
//package com.expedia.www.haystack.pipes.commons.kafka;
//
//import com.expedia.www.haystack.commons.config.Configuration;
//import org.cfg4j.provider.ConfigurationProvider;
//
//import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_KAFKA_CONFIG_PREFIX;
//
//public class KafkaConsumerConfig implements KafkaConfig {
//    private final KafkaConfig kafkaConfig;
//
//    public KafkaConfigurationProvider() {
//        final Configuration configuration = new Configuration();
//        final ConfigurationProvider configurationProvider = configuration.createMergeConfigurationProvider();
//        kafkaConfig = configurationProvider.bind(HAYSTACK_KAFKA_CONFIG_PREFIX, KafkaConfig.class);
//    }
//
//    @Override
//    public String getBrokers() {
//        return kafkaConfig.getBrokers();
//    }
//
//    @Override
//    public int getPort() {
//        return kafkaConfig.getPort();
//    }
//
//    @Override
//    public String getFromTopic() {
//        return kafkaConfig.getFromTopic();
//    }
//
//    @Override
//    public String getToTopic() {
//        return kafkaConfig.getToTopic();
//    }
//
//    @Override
//    public int getThreadCount() {
//        return kafkaConfig.getThreadCount();
//    }
//
//    @Override
//    public int getSessionTimeout() {
//        return kafkaConfig.getSessionTimeout();
//    }
//
//    @Override
//    public int getMaxWakeUps() {
//        return kafkaConfig.getMaxWakeUps();
//    }
//
//    @Override
//    public int getWakeUpTimeoutMs() {
//        return kafkaConfig.getWakeUpTimeoutMs();
//    }
//
//    @Override
//    public long getPollTimeoutMs() {
//        return kafkaConfig.getPollTimeoutMs();
//    }
//
//    @Override
//    public long getCommitMs() {
//        return kafkaConfig.getCommitMs();
//    }
//}
