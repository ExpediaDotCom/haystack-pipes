package com.expedia.www.haystack.pipes.kafka.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerConfigMap {
    private String brokers;
    private int port;
    private String topic;
    private String acks;// "-1": all replicas; "0": don't wait; "1": leader writes to its local log; "all": same as "-1"
    private int batchSize;
    private int lingerms;
    private int bufferMemory;

    public KafkaProducerConfigMap(String brokers, int port, String topic, String acks,
                                  int batchSize, int lingerms, int bufferMemory) {
        this.brokers = brokers;
        this.port = port;
        this.topic = topic;
        this.acks = acks;// "-1": all replicas; "0": don't wait; "1": leader writes to its local log; "all": same as "-1"
        this.batchSize = batchSize;
        this.lingerms = lingerms;
        this.bufferMemory = bufferMemory;
    }

    public Map<String, Object> getConfigurationMap() {
        final Map<String, Object> map = new HashMap<>();
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        map.put(ProducerConfig.ACKS_CONFIG, acks);
        map.put(ProducerConfig.RETRIES_CONFIG, 3);
        map.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        map.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        map.put(ProducerConfig.LINGER_MS_CONFIG, lingerms);
        map.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return map;
    }

}
