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
package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
import com.google.protobuf.util.JsonFormat;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.kafkaProducer.Constants.APPLICATION;

@Component
public class ProduceIntoExternalKafkaAction implements ForeachAction<String, Span> {
    private static final ExternalKafkaConfigurationProvider EKCP = new ExternalKafkaConfigurationProvider();
    private static final String TOPIC = EKCP.totopic();
    private static final String CLASS_NAME = ProduceIntoExternalKafkaAction.class.getSimpleName();
    private static final MetricObjects METRIC_OBJECTS = new MetricObjects();
    private static final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
    private static final String TOPIC_MESSAGE =
            "Loading ProduceIntoExternalKafkaAction with brokers [%s] port [%d] topic [%s]";
    static ObjectPool<ProduceIntoExternalKafkaCallback> OBJECT_POOL = new GenericObjectPool<>(
            new CallbackFactory(LoggerFactory.getLogger(ProduceIntoExternalKafkaCallback.class)));

    @VisibleForTesting static final String ERROR_MSG =
            "Exception posting JSON [%s] to Kafka; received message [%s]";
    @VisibleForTesting static Timer KAFKA_PRODUCER_POST = METRIC_OBJECTS.createAndRegisterBasicTimer(
            SUBSYSTEM, APPLICATION, CLASS_NAME, "KAFKA_PRODUCER_POST", TimeUnit.MICROSECONDS);
    @VisibleForTesting static Logger logger = LoggerFactory.getLogger(ProduceIntoExternalKafkaAction.class);

    private final Counter requestCounter;
    private final Counter postsInFlightCounter;

    static
    {
        logger.info(String.format(TOPIC_MESSAGE, EKCP.brokers(), EKCP.port(), TOPIC));
    }
    @VisibleForTesting static Factory factory = new Factory();

    static KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getConfigurationMap());

    private final TagFlattener tagFlattener = new TagFlattener();

    @Autowired
    public ProduceIntoExternalKafkaAction(Counter produceIntoExternalKafkaActionRequestCounter,
                                          Counter postsInFlightCounter) {
        requestCounter = produceIntoExternalKafkaActionRequestCounter;
        this.postsInFlightCounter = postsInFlightCounter;
    }

    public void apply(String key, Span value) {
        requestCounter.increment();
        final Stopwatch stopwatch = KAFKA_PRODUCER_POST.start();
        String jsonWithFlattenedTags = "";
        try {
            final String jsonWithOpenTracingTags = printer.print(value);
            jsonWithFlattenedTags = tagFlattener.flattenTags(jsonWithOpenTracingTags);
            final ProducerRecord<String, String> producerRecord =
                    factory.createProducerRecord(key, jsonWithFlattenedTags);

            final ProduceIntoExternalKafkaCallback callback = OBJECT_POOL.borrowObject(); // callback must returnObject()
            postsInFlightCounter.increment();
            // TODO Put the Span value into the callback so that it can write it to Kafka for retry

            kafkaProducer.send(producerRecord, callback);
        } catch (Exception exception) {
            // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
            final String message = String.format(ERROR_MSG, jsonWithFlattenedTags, exception.getMessage());
            logger.error(message, exception);
        } finally {
            stopwatch.stop();
        }
    }

    private static Map<String, Object> getConfigurationMap() {
        final Map<String, Object> map = new HashMap<>();
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, EKCP.brokers());
        map.put(ProducerConfig.ACKS_CONFIG, EKCP.acks());
        map.put(ProducerConfig.RETRIES_CONFIG, 3);
        map.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        map.put(ProducerConfig.BATCH_SIZE_CONFIG, EKCP.batchsize());
        map.put(ProducerConfig.LINGER_MS_CONFIG, EKCP.lingerms());
        map.put(ProducerConfig.BUFFER_MEMORY_CONFIG, EKCP.buffermemory());
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
