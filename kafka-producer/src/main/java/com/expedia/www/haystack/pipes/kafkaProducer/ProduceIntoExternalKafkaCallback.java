package com.expedia.www.haystack.pipes.kafkaProducer;

import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.kafka.clients.producer.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProduceIntoExternalKafkaCallback implements Callback {
    static final String DEBUG_MSG = "Successfully posted JSON to Kafka: topic [%s] partition [%d] offset [%d]";
    static final String ERROR_MSG = "Callback exception posting JSON to Kafka; received message [%s]";
    static Logger logger = LoggerFactory.getLogger(ProduceIntoExternalKafkaCallback.class);

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(metadata != null) { // means success, per https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/producer/Callback.html
            if(logger.isDebugEnabled()) {
                final String message = String.format(DEBUG_MSG,
                        metadata.topic(), metadata.partition(), metadata.offset());
                logger.debug(message);
            }
        }
        if(exception != null) {
            // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
            final String message = String.format(ERROR_MSG, exception.getMessage());
            logger.error(message, exception);
        }
    }
}
