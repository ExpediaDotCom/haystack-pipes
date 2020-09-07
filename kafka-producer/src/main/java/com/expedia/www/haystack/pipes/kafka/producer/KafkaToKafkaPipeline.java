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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KafkaToKafkaPipeline implements ForeachAction<String, Span> {
    @VisibleForTesting
    static final String ERROR_MSG =
            "Exception posting JSON [%s] to Kafka; received message [%s]";
    static ObjectPool<KafkaCallback> OBJECT_POOL = new GenericObjectPool<>(
            new CallbackFactory());
    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(KafkaToKafkaPipeline.class);
    @VisibleForTesting
    static Factory factory = new KafkaToKafkaPipeline.Factory();
    @VisibleForTesting
    static Counter kafkaProducerCounter;
    static Counter requestCounter;
    static Timer kafkaProducerTimer;
    private final TagFlattener tagFlattener = new TagFlattener();
    private final Map<SpanKeyExtractor, List<KafkaProducer<String, String>>> kafkaProducerMap;

    public KafkaToKafkaPipeline(MetricRegistry metricRegistry,
                                Map<SpanKeyExtractor, List<KafkaProducer<String, String>>> kafkaProducerMap) {
        this.kafkaProducerMap = kafkaProducerMap;
        requestCounter = metricRegistry.counter("REQUEST");
        kafkaProducerTimer = metricRegistry.timer("KAFKA_PRODUCER_POST_TIMER");
        kafkaProducerCounter = metricRegistry.counter("KAFKA_PRODUCER_POST_COUNTER");
    }

    @Override
    public void apply(String key, Span value) {
        requestCounter.inc();
        Timer.Context time = kafkaProducerTimer.time();
        kafkaProducerMap.forEach((spanKeyExtractor, kafkaProducers) -> {
            List<String> kafkaTopics = new ArrayList<>();
            final String kafkaKey = spanKeyExtractor.getKey();
            Optional<String> optionalMsg = spanKeyExtractor.extract(value);
            if (!optionalMsg.isPresent()) {
                logger.debug("Extractor skipped the span: {}", value);
                return;
            }
            String kafkaMsg = optionalMsg.get();
            kafkaTopics.addAll(spanKeyExtractor.getTopics());
            String msgWithFlattenedTags = tagFlattener.flattenTags(kafkaMsg);
            logger.info("KafkaProducer sending message: {},with key: {}  ", msgWithFlattenedTags, kafkaKey);
            kafkaProducers.forEach(kafkaProducer -> produceToKafkaTopics(kafkaProducer, kafkaTopics, kafkaKey, msgWithFlattenedTags));

        });
        time.stop();
    }

    public void produceToKafkaTopics(KafkaProducer<String, String> kafkaProducer, List<String> kafkaTopics, String kafkaKey, String jsonWithFlattenedTags) {
        kafkaTopics.forEach(topic -> {
            final ProducerRecord<String, String> producerRecord =
                    factory.createProducerRecord(topic, kafkaKey, jsonWithFlattenedTags);

            final KafkaCallback callback; // callback must returnObject()
            try {
                callback = OBJECT_POOL.borrowObject();
                kafkaProducerCounter.inc();
                // TODO Put the Span value into the callback so that it can write it to Kafka for retry
                kafkaProducer.send(producerRecord, callback);
                logger.info("Kafka message sent on topic: {}", topic);
            } catch (Exception exception) {
                // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
                final String message = String.format(ERROR_MSG, jsonWithFlattenedTags, exception.getMessage());
                logger.error(message, exception);
            }
        });
    }


    static class Factory {
        ProducerRecord<String, String> createProducerRecord(String topic, String key, String value) {
            return new ProducerRecord<>(topic, key, value);
        }

        KafkaProducer<String, String> createKafkaProducer(Map<String, Object> configurationMap) {
            return new KafkaProducer<>(configurationMap);
        }
    }

}
