package com.expedia.www.haystack.pipes.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;

/*
This class wraps kafka Producer with default topic defined in base.conf
 */
public class KafkaProducerWrapper {

    private KafkaProducer<String, String> kafkaProducer;
    private String defaultTopic;
    private KafkaProducerMetrics kafkaProducerMetrics;

    public KafkaProducerWrapper(KafkaProducer<String, String> kafkaProducer, String defaultTopic, KafkaProducerMetrics kafkaProducerMetrics) {
        this.kafkaProducer = kafkaProducer;
        this.defaultTopic = defaultTopic;
        this.kafkaProducerMetrics = kafkaProducerMetrics;
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public KafkaProducerMetrics getKafkaProducerMetrics() {
        return kafkaProducerMetrics;
    }
}
