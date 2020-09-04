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
package com.expedia.www.haystack.pipes.kafka.producer;

import com.netflix.servo.util.VisibleForTesting;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaCallback implements Callback {
    static final String DEBUG_MSG = "Successfully posted JSON to Kafka: topic [%s] partition [%d] offset [%d]";
    static final String ERROR_MSG_TEMPLATE = "Callback exception posting JSON to Kafka; received message [%s]";
    static final String POOL_ERROR_MSG_TEMPLATE = "Exception returning callback to pool; received message [%s]";

    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(KafkaCallback.class);


    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        try {
            if (metadata != null) { // means success, per https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/producer/Callback.html
                if (logger.isDebugEnabled()) {
                    final String message = String.format(DEBUG_MSG,
                            metadata.topic(), metadata.partition(), metadata.offset());
                    logger.debug(message);
                }
            }
            if (exception != null) {
                logError(exception, ERROR_MSG_TEMPLATE);
            }
        } finally {
            returnObjectToPoolButLogExceptionIfReturnFails();
        }
    }

    private void returnObjectToPoolButLogExceptionIfReturnFails() {
        try {
            KafkaToKafkaPipeline.kafkaProducerCounter.inc();
            KafkaToKafkaPipeline.OBJECT_POOL.returnObject(this);
        } catch (Exception exception) {
            logError(exception, POOL_ERROR_MSG_TEMPLATE);
        }
    }

    private void logError(Exception returnObjectException, String errorMessageTemplate) {
        // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
        final String message = String.format(errorMessageTemplate, returnObjectException.getMessage());
        logger.error(message, returnObjectException);
    }
}
