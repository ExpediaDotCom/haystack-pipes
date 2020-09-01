/*
 * Copyright 2020 Expedia, Inc.
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

import com.expedia.www.haystack.commons.config.ConfigurationLoader;
import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.commons.kafka.config.SpanKeyExtractorConfig;
import com.expedia.www.haystack.pipes.kafkaProducer.config.KafkaProducerConfig;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectConfiguration {

    private static ProjectConfiguration projectConfiguration = null;
    private static KafkaConsumerConfig kafkaConsumerConfig = null;
    private static List<KafkaProducerConfig> kafkaProducerConfigs = null;
    private static List<SpanKeyExtractorConfig> spanKeyExtractorConfigs = null;
    private final Config haystackConfig;

    private ProjectConfiguration() {
        String resourceName = System.getenv("configFilePath") == null ? "config/base.conf" : System.getenv("configFilePath");
        Config config = ConfigurationLoader.loadConfigFileWithEnvOverrides(resourceName, "HAYSTACK_PROP_");
        haystackConfig = config.getConfig("haystack");
    }

    public static ProjectConfiguration getInstance() {
        if (null == projectConfiguration) {
            projectConfiguration = new ProjectConfiguration();
        }
        return projectConfiguration;
    }

    public KafkaConsumerConfig getKafkaConsumerConfig() {
        if (null == kafkaConsumerConfig) {
            Config kafkaConfig = haystackConfig.getConfig("kafka");
            kafkaConsumerConfig = new KafkaConsumerConfig(kafkaConfig.getString("brokers"),
                    kafkaConfig.getInt("port"), kafkaConfig.getString("fromtopic"),
                    kafkaConfig.getString("totopic"), kafkaConfig.getInt("threadcount"),
                    kafkaConfig.getInt("sessiontimeout"), kafkaConfig.getInt("maxwakeups"),
                    kafkaConfig.getInt("wakeuptimeoutms"), kafkaConfig.getLong("polltimeoutms"),
                    kafkaConfig.getLong("commitms"));
        }
        return kafkaConsumerConfig;
    }

    public List<KafkaProducerConfig> getKafkaProducerConfigList() {
        if (null == kafkaProducerConfigs) {
            kafkaProducerConfigs = new ArrayList<>();
            List<Config> kafkaConfigs = (List<Config>) haystackConfig.getConfigList("externalKafkaList");
            kafkaConfigs.forEach(kafkaConfig -> {
                List<String> spanKeyExtractorStringList = kafkaConfig.getStringList("extractorList");
                kafkaProducerConfigs.add(new KafkaProducerConfig(kafkaConfig.getString("brokers"),
                        kafkaConfig.getInt("port"), kafkaConfig.getString("totopic"),
                        kafkaConfig.getString("acks"), kafkaConfig.getInt("batchsize"),
                        kafkaConfig.getInt("lingerms"), kafkaConfig.getInt("buffermemory"),
                        spanKeyExtractorStringList));
            });
        }
        return kafkaProducerConfigs;
    }


    public Map<String, Config> getSpanExtractorConfigs() {
        Map<String, Config> extractorConfigMap = new HashMap<>();
        if (haystackConfig != null) {
            List<Config> extractorConfigs = (List<Config>) haystackConfig.getConfigList("extractors");
            extractorConfigs.forEach(extractorConfig -> {
                String name = extractorConfig.getString("name");
                Config config = extractorConfig.getConfig("config");
                extractorConfigMap.put(name, config);
            });
        }
        return extractorConfigMap;
    }


}