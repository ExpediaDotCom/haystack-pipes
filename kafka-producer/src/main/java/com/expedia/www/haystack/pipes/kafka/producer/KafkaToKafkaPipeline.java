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

import com.codahale.metrics.Timer;
import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
import com.expedia.www.haystack.pipes.key.extractor.Record;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    static String kafkaProducerMsg = "Kafka message sent on topic: {}";
    private final TagFlattener tagFlattener = new TagFlattener();
    private List<KafkaProducerExtractorMapping> kafkaProducerExtractorMappings;

    public KafkaToKafkaPipeline(List<KafkaProducerExtractorMapping> kafkaProducerExtractorMappings) {
        this.kafkaProducerExtractorMappings = kafkaProducerExtractorMappings;
    }

    @Override
    public void apply(String key, Span value) {
        kafkaProducerExtractorMappings.forEach(kafkaProducerExtractorMapping -> {
            SpanKeyExtractor spanKeyExtractor = kafkaProducerExtractorMapping.getSpanKeyExtractor();

            List<Record> records = spanKeyExtractor.getRecords(value);
            if (CollectionUtils.isEmpty(records))
                logger.debug("Extractor skipped the span: {}", value);

            records.forEach(record -> {
                String message = record.getMessage();
                final String kafkaKey = record.getKey();
                String msgWithFlattenedTags = tagFlattener.flattenTags(message);
                kafkaProducerExtractorMapping.getKafkaProducerWrappers().forEach(kafkaProducerWrapper -> {
                    kafkaProducerWrapper.getKafkaProducerMetrics().incRequestCounter();
                    List<String> kafkaTopics = new ArrayList<>();
                    kafkaTopics.add(kafkaProducerWrapper.getDefaultTopic());
                    if (record.getProducerTopicsMappings() == null || !record.getProducerTopicsMappings().containsKey(kafkaProducerWrapper.getName())) {
                        logger.error("Extractor skipped the span: {}, as no topics found for producer: {}", value, kafkaProducerWrapper.getName());
                        return;
                    }
                    logger.debug("Kafka Producer sending message: {},with key: {}  ", msgWithFlattenedTags, kafkaKey);
                    kafkaTopics.addAll(record.getProducerTopicsMappings().get(kafkaProducerWrapper.getName()));
                    produceToKafkaTopics(kafkaProducerWrapper.getKafkaProducer(), kafkaTopics, kafkaKey, msgWithFlattenedTags,
                            kafkaProducerWrapper.getKafkaProducerMetrics());
                });
            });
        });

    }

    public void produceToKafkaTopics(KafkaProducer<String, String> kafkaProducer, List<String> kafkaTopics, String kafkaKey,
                                     String jsonWithFlattenedTags, KafkaProducerMetrics kafkaProducerMetrics) {
        Timer.Context time = kafkaProducerMetrics.getTimer().time();
        kafkaTopics.forEach(topic -> {
            final ProducerRecord<String, String> producerRecord =
                    factory.createProducerRecord(topic, kafkaKey, jsonWithFlattenedTags);

            final KafkaCallback callback; // callback must returnObject()
            try {
                callback = OBJECT_POOL.borrowObject();
                kafkaProducerMetrics.incSuccessCounter();
                // TODO Put the Span value into the callback so that it can write it to Kafka for retry
                kafkaProducer.send(producerRecord, callback);
                logger.debug(kafkaProducerMsg, topic);
            } catch (Exception exception) {
                kafkaProducerMetrics.incFailureCounter();
                // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
                final String message = String.format(ERROR_MSG, jsonWithFlattenedTags, exception.getMessage());
                logger.error(message, exception);
            }
        });
        time.stop();
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
