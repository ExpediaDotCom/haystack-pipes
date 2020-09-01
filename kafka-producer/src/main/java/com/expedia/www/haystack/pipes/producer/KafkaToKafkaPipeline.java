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
package com.expedia.www.haystack.pipes.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.producer.config.KafkaProducerConfig;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    private final TagFlattener tagFlattener = new TagFlattener();
    private final Counter requestCounter;
    @VisibleForTesting
    static Counter kafkaProducerCounter;
    private final Timer kafkaProducerTimer;
    private List<KafkaProducerConfig> kafkaProducerConfigMaps;
    private List<SpanKeyExtractor> spanKeyExtractors;
    @VisibleForTesting
    static Map<KafkaProducer<String, String>, List<SpanKeyExtractor>> kafkaProducerSpanExtractorMap = new HashMap<>();

    public KafkaToKafkaPipeline(MetricRegistry metricRegistry,
                                ProjectConfiguration projectConfiguration,
                                List<SpanKeyExtractor> spanKeyExtractors) {
        this.kafkaProducerConfigMaps = projectConfiguration.getKafkaProducerConfigList();
        this.spanKeyExtractors = spanKeyExtractors;
        this.requestCounter = metricRegistry.counter("REQUEST");
        this.kafkaProducerTimer = metricRegistry.timer("KAFKA_PRODUCER_POST_TIMER");
        this.kafkaProducerCounter = metricRegistry.counter("KAFKA_PRODUCER_POST_COUNTER");
    }

    @Override
    public void apply(String key, Span value) {
        requestCounter.inc();
        Timer.Context time = kafkaProducerTimer.time();
        //kafkaProducers = getKafkaProducers(kafkaProducerConfigMaps);
        createKafkaProducersExtractorMap(kafkaProducerConfigMaps);
        kafkaProducerSpanExtractorMap.forEach((kafkaProducer, spanKeyExtractors) -> {
            spanKeyExtractors.forEach(spanKeyExtractor -> {
                List<String> kafkaTopics = new ArrayList<>();
                final String kafkaKey = spanKeyExtractor.getKey();
                String kafkaMsg = spanKeyExtractor.extract(value).get();
                if (StringUtils.isEmpty(kafkaKey)) {
                    return;
                }
                if (spanKeyExtractor != null && spanKeyExtractor.getTopics() != null)
                    kafkaTopics.addAll(spanKeyExtractor.getTopics());
                String msgWithFlattenedTags = tagFlattener.flattenTags(kafkaMsg);
                logger.info("KafkaProducer sending message: {},with key: {}  ", msgWithFlattenedTags, kafkaKey);
                produceToKafkaTopics(kafkaProducer, kafkaTopics, kafkaKey, msgWithFlattenedTags);
            });
        });
//        kafkaProducers.stream().forEach(kafkaProducer -> {
//            kafkaProducerSpanExtractorMap.get(kafkaProducer).forEach(spanKeyExtractor -> {
//                List<String> kafkaTopics = new ArrayList<>();
//                final String kafkaKey = getKafkaMessageKey(key, spanKeyExtractor);
//                String kafkaMsg = getKafkaMessage(value, spanKeyExtractor);
//                if (StringUtils.isEmpty(kafkaKey)) {
//                    return;
//                }
//                if (spanKeyExtractor != null && spanKeyExtractor.getTopics() != null)
//                    kafkaTopics.addAll(spanKeyExtractor.getTopics());
//                logger.info("KafkaProducer sending message: {},with key: {}  ", kafkaMsg, kafkaKey);
//                String jsonWithFlattenedTags = tagFlattener.flattenTags(kafkaMsg);
//                produceToKafkaTopics(kafkaProducer, kafkaTopics, kafkaKey, jsonWithFlattenedTags);
//            });
//
//        });
        time.stop();
    }

    private void produceToKafkaTopics(KafkaProducer<String, String> kafkaProducer, List<String> kafkaTopics, String kafkaKey, String jsonWithFlattenedTags) {
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

//    private String getKafkaMessage(Span value, SpanKeyExtractor spanKeyExtractor) {
//        return spanKeyExtractor.extract(value).get();
//    }
//
//    private String getKafkaMessageKey(String key, SpanKeyExtractor spanKeyExtractor) {
//        return spanKeyExtractor.getKey();
//    }

//    private List<KafkaProducer<String, String>> getKafkaProducers(List<KafkaProducerConfig> kafkaProducerConfigMaps) {
//        List<KafkaProducer<String, String>> kafkaProducerList = new ArrayList<>();
//        kafkaProducerConfigMaps.forEach(kafkaProducerConfigMap -> {
//            final KafkaProducer<String, String> kafkaProducer = factory.createKafkaProducer(kafkaProducerConfigMap.getConfigurationMap());
//            kafkaProducerList.add(kafkaProducer);
//            List<String> spanExtractorStringList = kafkaProducerConfigMap.getSpanKeyExtractorStringList();
//            List<SpanKeyExtractor> spanKeyExtractorList = spanKeyExtractors.stream()
//                    .filter(spanKeyExtractor -> spanExtractorStringList.contains(spanKeyExtractor.name()))
//                    .collect(Collectors.toList());
//            kafkaProducerSpanExtractorMap.put(kafkaProducer, spanKeyExtractorList);
//        });
//        return kafkaProducerList;
//    }

    private Map<KafkaProducer<String, String>, List<SpanKeyExtractor>> createKafkaProducersExtractorMap(List<KafkaProducerConfig> kafkaProducerConfigMaps) {
        kafkaProducerConfigMaps.forEach(kafkaProducerConfigMap -> {
            final KafkaProducer<String, String> kafkaProducer = factory.createKafkaProducer(kafkaProducerConfigMap.getConfigurationMap());
            List<String> spanExtractorStringList = kafkaProducerConfigMap.getSpanKeyExtractorStringList();
            List<SpanKeyExtractor> spanKeyExtractorList = spanKeyExtractors.stream()
                    .filter(spanKeyExtractor -> spanExtractorStringList.contains(spanKeyExtractor.name()))
                    .collect(Collectors.toList());
            kafkaProducerSpanExtractorMap.put(kafkaProducer, spanKeyExtractorList);
        });
        return kafkaProducerSpanExtractorMap;
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
