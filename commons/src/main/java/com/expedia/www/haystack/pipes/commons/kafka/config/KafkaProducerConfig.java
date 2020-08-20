package com.expedia.www.haystack.pipes.commons.kafka.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerConfig {

    private String brokers;

    private int port;

    private String toTopic;

    private String acks;

    private int batchSize;

    private int lingerMs;

    private int bufferMemory;

    public KafkaProducerConfig(final String brokers, final int port, final String toTopic, final String acks, final int batchSize, final int lingerMs, final int bufferMemory) {
        this.brokers = brokers;
        this.port = port;
        this.toTopic = toTopic;
        this.acks = acks;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.bufferMemory = bufferMemory;
    }

    public String getBrokers() {
        return this.brokers;
    }

    public int getPort() {
        return this.port;
    }

    public String getToTopic() {
        return this.toTopic;
    }

    public String getAcks() {
        return this.acks;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public int getLingerMs() {
        return this.lingerMs;
    }

    public int getBufferMemory() {
        return this.bufferMemory;
    }

    public Map<String, Object> getConfigurationMap() {
        final Map<String, Object> map = new HashMap<>();
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.getBrokers());
        map.put(ProducerConfig.ACKS_CONFIG, getAcks());
        map.put(ProducerConfig.RETRIES_CONFIG, 3);
        map.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        map.put(ProducerConfig.BATCH_SIZE_CONFIG, getBatchSize());
        map.put(ProducerConfig.LINGER_MS_CONFIG, getLingerMs());
        map.put(ProducerConfig.BUFFER_MEMORY_CONFIG, this.getBufferMemory());
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return map;
    }
}
