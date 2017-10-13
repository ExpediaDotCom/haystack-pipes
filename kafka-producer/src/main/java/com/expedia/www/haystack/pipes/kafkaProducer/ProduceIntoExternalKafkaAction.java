/*
 * Copyright 2017 Expedia, Inc.
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

import com.expedia.www.haystack.metrics.MetricObjects;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.kafkaProducer.Constants.APPLICATION;

public class ProduceIntoExternalKafkaAction implements ForeachAction<String, String> {
    private static final ExternalKafkaConfigurationProvider EKCP = new ExternalKafkaConfigurationProvider();
    private static final String TOPIC = EKCP.toTopic();
    private static final String CLASS_NAME = ProduceIntoExternalKafkaAction.class.getSimpleName();
    private static final MetricObjects METRIC_OBJECTS = new MetricObjects();
    static final String ERROR_MSG = "Problem posting JSON [%s] to Kafka";
    static final String DEBUG_MSG = "Sent JSON [%s] to Kafka topic [%s]";
    static final Counter REQUEST = METRIC_OBJECTS.createAndRegisterCounter(SUBSYSTEM, APPLICATION, CLASS_NAME, "REQUEST");
    static final Counter ERROR = METRIC_OBJECTS.createAndRegisterCounter(SUBSYSTEM, APPLICATION, CLASS_NAME, "ERROR");
    static Timer KAFKA_PRODUCER_POST = METRIC_OBJECTS.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION, CLASS_NAME,
            "KAFKA_PRODUCER_POST", TimeUnit.MICROSECONDS);
    static Logger logger = LoggerFactory.getLogger(ProduceIntoExternalKafkaAction.class);
    static Factory factory = new Factory();

    static KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getConfigurationMap());

    public void apply(String key, String value) {
        REQUEST.increment();
        final Stopwatch stopwatch = KAFKA_PRODUCER_POST.start();
        try {
            final ProducerRecord<String, String> producerRecord = factory.createProducerRecord(key, value);
            final Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send(producerRecord);
            final RecordMetadata recordMetadata = recordMetadataFuture.get();
            if(logger.isDebugEnabled()) {
                logger.debug(String.format(DEBUG_MSG, value, recordMetadata.partition()));
            }
        } catch (Exception exception) {
            ERROR.increment();
            logger.error(ERROR_MSG, value, exception);
        } finally {
            stopwatch.stop();
        }
    }

    private static Map<String, Object> getConfigurationMap() {
        final Map<String, Object> map = new HashMap<>();
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, EKCP.brokers() + ":" + EKCP.port());
        map.put(ProducerConfig.ACKS_CONFIG, EKCP.acks());
        map.put(ProducerConfig.RETRIES_CONFIG, 0);
        map.put(ProducerConfig.BATCH_SIZE_CONFIG, EKCP.batchSize());
        map.put(ProducerConfig.LINGER_MS_CONFIG, EKCP.lingerMs());
        map.put(ProducerConfig.BUFFER_MEMORY_CONFIG, EKCP.bufferMemory());
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return map;
    }

    static class Factory {
        ProducerRecord<String, String> createProducerRecord(String key, String value) {
            return new ProducerRecord<>(TOPIC, key, value);
        }
    }
}
