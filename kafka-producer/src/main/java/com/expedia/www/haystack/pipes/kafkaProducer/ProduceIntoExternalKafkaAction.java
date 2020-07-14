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
import com.expedia.www.haystack.pipes.commons.TimersAndCounters;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.config.SpanKeyExtractorConfig;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.config.SpanKeyExtractorConfigProvider;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.loader.SpanKeyExtractorLoader;
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
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

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class ProduceIntoExternalKafkaAction implements ForeachAction<String, Span> {
    private static final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
    @VisibleForTesting
    static final String TOPIC_MESSAGE =
            "Loading ProduceIntoExternalKafkaAction with brokers [%s] port [%d] topic [%s]";
    static AtomicReference<TimersAndCounters> COUNTERS_AND_TIMER = new AtomicReference<>(null);
    static ObjectPool<ProduceIntoExternalKafkaCallback> OBJECT_POOL = new GenericObjectPool<>(
            new CallbackFactory(LoggerFactory.getLogger(ProduceIntoExternalKafkaCallback.class)));

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
    private SpanKeyExtractorConfig spanKeyExtractorConfig;

    @Autowired
    public ProduceIntoExternalKafkaAction(Factory produceIntoExternalKafkaActionFactory,
                                          TimersAndCounters timersAndCounters,
                                          Logger produceIntoExternalKafkaActionLogger,
                                          ExternalKafkaConfigurationProvider externalKafkaConfigurationProvider,
                                          SpanKeyExtractorConfigProvider spanKeyExtractorConfigProvider) {
        this.factory = produceIntoExternalKafkaActionFactory;
        this.timersAndCounters = timersAndCounters;
        COUNTERS_AND_TIMER.compareAndSet(null, timersAndCounters);
        this.logger = produceIntoExternalKafkaActionLogger;

        final Map<String, Object> configurationMap = externalKafkaConfigurationProvider.getConfigurationMap();
        this.kafkaProducer = factory.createKafkaProducer(configurationMap);
        this.topic = externalKafkaConfigurationProvider.totopic();
        this.spanKeyExtractorConfig = spanKeyExtractorConfigProvider.getSpanKeyExtractorConfig();
        logger.info(String.format(TOPIC_MESSAGE, externalKafkaConfigurationProvider.brokers(),
                externalKafkaConfigurationProvider.port(), topic));
    }

    @Override
    public void apply(String key, Span value) {
        timersAndCounters.incrementRequestCounter();
        final Stopwatch stopwatch = timersAndCounters.startTimer();
        String jsonWithFlattenedTags = "";
        try {
            final String jsonWithOpenTracingTags;
            if (spanKeyExtractorConfig != null) {
                jsonWithOpenTracingTags = loadExtractorAndApply(value);
                if (jsonWithFlattenedTags == null) {
                    return;
                }
            } else {
                jsonWithOpenTracingTags = printer.print(value);
            }
            jsonWithFlattenedTags = tagFlattener.flattenTags(jsonWithOpenTracingTags);
            final ProducerRecord<String, String> producerRecord =
                    factory.createProducerRecord(topic, key, jsonWithFlattenedTags);

            final ProduceIntoExternalKafkaCallback callback = OBJECT_POOL.borrowObject(); // callback must returnObject()
            timersAndCounters.incrementCounter(POSTS_IN_FLIGHT_COUNTER_INDEX);
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

    private String loadExtractorAndApply(Span span) {
        SpanKeyExtractorLoader spanKeyExtractorLoader = SpanKeyExtractorLoader.getInstance(spanKeyExtractorConfig, "kafka");
        SpanKeyExtractor spanKeyExtractor = spanKeyExtractorLoader.getSpanKeyExtractor();
        if (spanKeyExtractor == null) {
            return null;
        }
        return spanKeyExtractor.extract(span);
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
