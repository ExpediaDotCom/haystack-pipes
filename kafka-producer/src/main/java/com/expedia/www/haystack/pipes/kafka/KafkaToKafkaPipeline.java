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
package com.expedia.www.haystack.pipes.kafka;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.TimersAndCounters;
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
import com.expedia.www.haystack.pipes.commons.key.extractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.commons.key.extractor.config.SpanKeyExtractorConfigProvider;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class KafkaToKafkaPipeline implements ForeachAction<String, Span> {
    private static final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
    @VisibleForTesting
    static final String TOPIC_MESSAGE =
            "Loading ProduceIntoExternalKafkaAction with brokers [%s] port [%d] topic [%s]";
    static AtomicReference<TimersAndCounters> COUNTERS_AND_TIMER = new AtomicReference<>(null);
    static ObjectPool<KafkaCallback> OBJECT_POOL = new GenericObjectPool<>(
            new CallbackFactory(LoggerFactory.getLogger(KafkaCallback.class)));

    @VisibleForTesting
    static final String ERROR_MSG =
            "Exception posting JSON [%s] to Kafka; received message [%s]";
    @VisibleForTesting
    static final int POSTS_IN_FLIGHT_COUNTER_INDEX = 0;
    private final Factory factory;
    private final TimersAndCounters timersAndCounters;
    private final Logger logger;
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    private final TagFlattener tagFlattener = new TagFlattener();
    private SpanKeyExtractor spanKeyExtractor;

    @Autowired
    public KafkaToKafkaPipeline(Factory produceIntoExternalKafkaActionFactory,
                                TimersAndCounters timersAndCounters,
                                Logger kafkaToExternalKafkaActionLogger,
                                KafkaProducerConfigurationProvider kafkaConfigurationProvider,
                                SpanKeyExtractorConfigProvider spanKeyExtractorConfigProvider) {
        this.factory = produceIntoExternalKafkaActionFactory;
        this.timersAndCounters = timersAndCounters;
        COUNTERS_AND_TIMER.compareAndSet(null, timersAndCounters);
        this.logger = kafkaToExternalKafkaActionLogger;

        final Map<String, Object> configurationMap = kafkaConfigurationProvider.getConfigurationMap();
        this.kafkaProducer = factory.createKafkaProducer(configurationMap);
        this.topic = kafkaConfigurationProvider.totopic();
        this.spanKeyExtractor = spanKeyExtractorConfigProvider.loadAndGetSpanExtractor();
        logger.info(String.format(TOPIC_MESSAGE, kafkaConfigurationProvider.brokers(),
                kafkaConfigurationProvider.port(), topic));
    }

    @Override
    public void apply(String key, Span value) {
        timersAndCounters.incrementRequestCounter();
        final Stopwatch stopwatch = timersAndCounters.startTimer();
        List<String> kafkaTopics = new ArrayList<>();
        kafkaTopics.add(topic);
        final String kafkaKey = getKafkaMessageKey(key);

        String jsonWithOpenTracingTags = getMessageOpenTracingTags(value);
        if (jsonWithOpenTracingTags == null) {
            return;
        }
        if (spanKeyExtractor != null && spanKeyExtractor.getTopics() != null)
            kafkaTopics.addAll(spanKeyExtractor.getTopics());

        String jsonWithFlattenedTags = tagFlattener.flattenTags(jsonWithOpenTracingTags);
        kafkaTopics.forEach(topic -> {
            final ProducerRecord<String, String> producerRecord =
                    factory.createProducerRecord(topic, kafkaKey, jsonWithFlattenedTags);

            final KafkaCallback callback; // callback must returnObject()
            try {
                callback = OBJECT_POOL.borrowObject();
                timersAndCounters.incrementCounter(POSTS_IN_FLIGHT_COUNTER_INDEX);
                // TODO Put the Span value into the callback so that it can write it to Kafka for retry
                kafkaProducer.send(producerRecord, callback);
            } catch (Exception exception) {
                // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
                final String message = String.format(ERROR_MSG, jsonWithFlattenedTags, exception.getMessage());
                logger.error(message, exception);
            }
        });

        stopwatch.stop();
    }

    private String getMessageOpenTracingTags(Span value) {
        if (spanKeyExtractor != null)
            return spanKeyExtractor.extract(value);
        String jsonWithFlattenedTags = null;
        try {
            jsonWithFlattenedTags = printer.print(value);
        } catch (InvalidProtocolBufferException exception) {
            final String message = String.format(ERROR_MSG, value, exception.getMessage());
            logger.error(message, exception);
        }
        return jsonWithFlattenedTags;
    }

    private String getKafkaMessageKey(String key) {
        if (spanKeyExtractor != null && spanKeyExtractor.getKey() != null) {
            return spanKeyExtractor.getKey();
        }
        return key;
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
