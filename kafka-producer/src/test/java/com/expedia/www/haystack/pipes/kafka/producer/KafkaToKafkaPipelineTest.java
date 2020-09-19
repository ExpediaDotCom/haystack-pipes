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

import java.util.List;
import java.util.Optional;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.*;
import static java.util.Collections.singletonList;
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
    private Logger mockLogger;
    @Mock
    private Timer mockKafkaProducerTimer;
    @Mock
    private KafkaProducer<String, String> mockKafkaProducer;
    @Mock
    private SpanKeyExtractor mockSpanKeyExtractor;
    @Mock
    private KafkaProducerMetrics mockKafkaProducerMetrics;

    private Logger realLogger;

    @Before
    public void setUp() {
        realLogger = KafkaToKafkaPipeline.logger;
        whenForConstructor();
        List<KafkaProducerExtractorMapping> kafkaProducerExtractorMappings = singletonList(new KafkaProducerExtractorMapping(mockSpanKeyExtractor,
                singletonList(new KafkaProducerWrapper(mockKafkaProducer, "mock-Topic", mockKafkaProducerMetrics))));
        kafkaToKafkaPipeline = new KafkaToKafkaPipeline(kafkaProducerExtractorMappings);
    }

    private void whenForConstructor() {
        when(mockSpanKeyExtractor.extract(any())).thenReturn(Optional.empty());
        when(mockMetricRegistry.timer(anyString())).thenReturn(mockKafkaProducerTimer);
        when(mockKafkaProducerTimer.time()).thenReturn(mockTimer);
        when(mockKafkaProducerMetrics.getTimer()).thenReturn(mockKafkaProducerTimer);
        when(mockSpanKeyExtractor.getKey()).thenReturn("externalKafkaKey");
    }

    @Test
    public void testProduceToKafkaTopics() {
        KafkaToKafkaPipeline.logger = mockLogger;
        kafkaToKafkaPipeline.produceToKafkaTopics(mockKafkaProducer, singletonList("mock-Topic"), "mock-key", JSON_SPAN_STRING_WITH_FLATTENED_TAGS, mockKafkaProducerMetrics);
        verify(mockLogger).debug(KafkaToKafkaPipeline.kafkaProducerMsg, "mock-Topic");
        KafkaToKafkaPipeline.logger = realLogger;
    }

    @Test
    public void testApplyWithNullMessage() {
        List<KafkaProducerExtractorMapping> kafkaProducerExtractorMappings = singletonList(new KafkaProducerExtractorMapping(mockSpanKeyExtractor,
                singletonList(new KafkaProducerWrapper(mockKafkaProducer, "defaultTopic", mockKafkaProducerMetrics))));
        KafkaToKafkaPipeline mockKafkaToKafkaPipeline = new KafkaToKafkaPipeline(kafkaProducerExtractorMappings);
        when(mockSpanKeyExtractor.getKey()).thenReturn("mock-Key");
        when(mockSpanKeyExtractor.extract(FULLY_POPULATED_SPAN)).thenReturn(Optional.empty());
        KafkaToKafkaPipeline.logger = mockLogger;

        mockKafkaToKafkaPipeline.apply(null, FULLY_POPULATED_SPAN);
        verify(mockLogger).debug("Extractor skipped the span: {}", FULLY_POPULATED_SPAN);
        KafkaToKafkaPipeline.logger = realLogger;
    }

    @Test
    public void testProduceToKafkaTopicsWithException() {
        KafkaToKafkaPipeline.logger = mockLogger;
        Exception exception = new RuntimeException();
        when(mockKafkaProducer.send(any(), any())).thenThrow(exception);
        kafkaToKafkaPipeline.produceToKafkaTopics(mockKafkaProducer, singletonList("mock-Topic"), "mock-key", JSON_SPAN_STRING_WITH_FLATTENED_TAGS, mockKafkaProducerMetrics);
        verify(mockLogger).error(String.format(KafkaToKafkaPipeline.ERROR_MSG, JSON_SPAN_STRING_WITH_FLATTENED_TAGS, null), exception);
        KafkaToKafkaPipeline.logger = realLogger;
    }

    @Test
    public void testProduceToKafkaTopicsForCallback() {
        KafkaToKafkaPipeline.logger = mockLogger;
        Exception exception = new RuntimeException();

        when(mockKafkaProducer.send(any(), any())).thenThrow(exception);
        kafkaToKafkaPipeline.produceToKafkaTopics(mockKafkaProducer, singletonList("mock-Topic"), "mock-key", JSON_SPAN_STRING_WITH_FLATTENED_TAGS, mockKafkaProducerMetrics);

        verify(mockLogger, times(1)).error(String.format(KafkaToKafkaPipeline.ERROR_MSG, JSON_SPAN_STRING_WITH_FLATTENED_TAGS, null), exception);
        KafkaToKafkaPipeline.logger = realLogger;
    }

    @Test
    public void testApplyWithTags() {
        Logger realLogger = KafkaToKafkaPipeline.logger;
        when(mockSpanKeyExtractor.extract(any())).thenReturn(Optional.of(JSON_SPAN_STRING));
        KafkaToKafkaPipeline.logger = mockLogger;
        kafkaToKafkaPipeline.apply(null, FULLY_POPULATED_SPAN);
        KafkaToKafkaPipeline.logger = realLogger;
        verify(mockLogger).debug("KafkaProducer sending message: {},with key: {}  ", JSON_SPAN_STRING_WITH_FLATTENED_TAGS, "externalKafkaKey");
    }

    @Test
    public void testApplyWithoutTags() {
        Logger realLogger = KafkaToKafkaPipeline.logger;
        KafkaToKafkaPipeline.logger = mockLogger;
        when(mockSpanKeyExtractor.extract(any())).thenReturn(Optional.of(JSON_SPAN_STRING_WITH_NO_TAGS));
        kafkaToKafkaPipeline.apply(null, NO_TAGS_SPAN);
        KafkaToKafkaPipeline.logger = realLogger;
        verify(mockLogger).debug("KafkaProducer sending message: {},with key: {}  ", JSON_SPAN_STRING_WITH_NO_TAGS, "externalKafkaKey");
    }

    @Test
    public void testFactoryCreateProducerRecord() {
        final ProducerRecord<String, String> producerRecord = KafkaToKafkaPipeline.factory.createProducerRecord(TOPIC, KEY, VALUE);
        assertEquals(KEY, producerRecord.key());
        assertEquals(VALUE, producerRecord.value());
    }
}