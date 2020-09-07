/*
 * Copyright 2020 Expedia, Inc.
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
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.*;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaToKafkaPipelineTest {

    private static final String TOPIC = RANDOM.nextLong() + "TOPIC";
    private static final String KEY = RANDOM.nextLong() + "KEY";
    private static final String VALUE = RANDOM.nextLong() + "VALUE";
    @Mock
    Timer.Context mockTimer;
    private KafkaToKafkaPipeline kafkaToKafkaPipeline;
    @Mock
    private MetricRegistry mockMetricRegistry;
    @Mock
    private Counter mockRequestCounter;
    @Mock
    private Logger mockLogger;
    @Mock
    private Timer mockKafkaProducerTimer;
    @Mock
    private Counter mockKafkaProducerCounter;
    @Mock
    private KafkaProducer<String, String> mockKafkaProducer;
    @Mock
    private SpanKeyExtractor mockSpanKeyExtractor;

    private Logger realLogger;

    @Before
    public void setUp() throws Exception {
        realLogger = KafkaToKafkaPipeline.logger;
        when(mockMetricRegistry.counter("REQUEST")).thenReturn(mockRequestCounter);
        when(mockMetricRegistry.counter("KAFKA_PRODUCER_POST_COUNTER")).thenReturn(mockKafkaProducerCounter);
        when(mockMetricRegistry.timer(anyString())).thenReturn(mockKafkaProducerTimer);
        when(mockKafkaProducerTimer.time()).thenReturn(mockTimer);
        kafkaToKafkaPipeline = new KafkaToKafkaPipeline(mockMetricRegistry,
                Service.getExtractorKafkaProducerMap(ProjectConfiguration.getInstance()));
    }

    @Test
    public void testProduceToKafkaTopics() {
        KafkaToKafkaPipeline.logger = mockLogger;
        kafkaToKafkaPipeline.produceToKafkaTopics(mockKafkaProducer, Arrays.asList("mock-Topic"), "mock-key", JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
        verify(mockLogger).info("Kafka message sent on topic: {}", "mock-Topic");
        verify(mockKafkaProducerCounter).inc();
        KafkaToKafkaPipeline.logger = realLogger;
    }

    @Test
    public void testApplyWithNullMessage() {
        Map<SpanKeyExtractor, List<KafkaProducer<String, String>>> keyExtractorMap = new HashMap<>();
        keyExtractorMap.put(mockSpanKeyExtractor, Arrays.asList(mockKafkaProducer));
        KafkaToKafkaPipeline mockKafkaToKafkaPipeline = new KafkaToKafkaPipeline(mockMetricRegistry,
                keyExtractorMap);
        when(mockSpanKeyExtractor.getKey()).thenReturn("mock-Key");
        when(mockSpanKeyExtractor.extract(FULLY_POPULATED_SPAN)).thenReturn(Optional.empty());
        KafkaToKafkaPipeline.logger = mockLogger;

        mockKafkaToKafkaPipeline.apply(null, FULLY_POPULATED_SPAN);
        verify(mockLogger).info("Extractor skipped the span: {}", FULLY_POPULATED_SPAN);
        KafkaToKafkaPipeline.logger = realLogger;
    }

    @Test
    public void testProduceToKafkaTopicsWithException() {
        KafkaToKafkaPipeline.logger = mockLogger;
        Exception exception = new RuntimeException();
        when(mockKafkaProducer.send(any(), any())).thenThrow(exception);
        kafkaToKafkaPipeline.produceToKafkaTopics(mockKafkaProducer, Arrays.asList("mock-Topic"), "mock-key", JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
        verify(mockLogger).error(String.format(KafkaToKafkaPipeline.ERROR_MSG, JSON_SPAN_STRING_WITH_FLATTENED_TAGS, null), exception);
        KafkaToKafkaPipeline.logger = realLogger;
    }

    @Test
    public void testApplyWithTags() {
        Logger realLogger = KafkaToKafkaPipeline.logger;
        KafkaToKafkaPipeline.logger = mockLogger;
        kafkaToKafkaPipeline.apply(null, FULLY_POPULATED_SPAN);
        KafkaToKafkaPipeline.logger = realLogger;
        verify(mockRequestCounter).inc();
        verify(mockKafkaProducerTimer).time();
        verify(mockRequestCounter).inc();
        verify(mockLogger).info("KafkaProducer sending message: {},with key: {}  ", JSON_SPAN_STRING_WITH_FLATTENED_TAGS, "externalKafkaKey");
    }

    @Test
    public void testApplyWithoutTags() {
        Logger realLogger = KafkaToKafkaPipeline.logger;
        KafkaToKafkaPipeline.logger = mockLogger;
        kafkaToKafkaPipeline.apply(null, NO_TAGS_SPAN);
        KafkaToKafkaPipeline.logger = realLogger;
        verify(mockRequestCounter).inc();
        verify(mockKafkaProducerTimer).time();
        verify(mockRequestCounter).inc();
        verify(mockLogger).info("KafkaProducer sending message: {},with key: {}  ", JSON_SPAN_STRING_WITH_NO_TAGS, "externalKafkaKey");
    }

    @Test
    public void testFactoryCreateProducerRecord() {
        final ProducerRecord<String, String> producerRecord = KafkaToKafkaPipeline.factory.createProducerRecord(TOPIC, KEY, VALUE);
        assertEquals(KEY, producerRecord.key());
        assertEquals(VALUE, producerRecord.value());
    }
}