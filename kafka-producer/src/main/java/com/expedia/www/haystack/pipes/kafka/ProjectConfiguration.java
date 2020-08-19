package com.expedia.www.haystack.pipes.kafka;

import com.expedia.www.haystack.commons.config.ConfigurationLoader;
import com.expedia.www.haystack.pipes.kafka.config.KafkaProducerConfigMap;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;

public class ProjectConfiguration {

    private final Config haystackConfig;

    public ProjectConfiguration() {
        Config config = ConfigurationLoader.loadConfigFileWithEnvOverrides("config/base.conf", "HAYSTACK_PROP_");
        haystackConfig = config.getConfig("haystack");
    }

    public List<KafkaProducerConfigMap> getKafkaProducerConfigList() {
        List<KafkaProducerConfigMap> externalKafkaConfigList = new ArrayList<>();
        List<Config> externalKafkaConfigs = (List<Config>) haystackConfig.getConfigList("externalKafka");
        externalKafkaConfigs.forEach(config -> {
            Config kafkaConfig = config.getConfig("config");
            String brokers = kafkaConfig.getString("brokers");
            int port = kafkaConfig.getInt("port");
            String topic = kafkaConfig.getString("topic");
            String acks = kafkaConfig.getString("acks");
            int batchSize = kafkaConfig.getInt("batchSize");
            int lingerms = kafkaConfig.getInt("lingerms");
            int bufferMemory = kafkaConfig.getInt("bufferMemory");
            KafkaProducerConfigMap kafkaProducerConfigMap = new KafkaProducerConfigMap(brokers, port, topic, acks,
                    batchSize, lingerms, bufferMemory);
            externalKafkaConfigList.add(kafkaProducerConfigMap);
        });
        return externalKafkaConfigList;
    }
}
