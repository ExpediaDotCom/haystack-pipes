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
import com.expedia.www.haystack.pipes.key.extractor.Record;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collections;

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
        kafkaToKafkaPipeline = new KafkaToKafkaPipeline(
                singletonList(mockSpanKeyExtractor),
                singletonList(new KafkaProducerWrapper("mockKafkaProducer", "mock-Topic", mockKafkaProducer, mockKafkaProducerMetrics))
        );
    }

    private void whenForConstructor() {
        when(mockSpanKeyExtractor.getRecords(any())).thenReturn(Collections.EMPTY_LIST);
        when(mockMetricRegistry.timer(anyString())).thenReturn(mockKafkaProducerTimer);
        when(mockKafkaProducerTimer.time()).thenReturn(mockTimer);
        when(mockKafkaProducerMetrics.getTimer()).thenReturn(mockKafkaProducerTimer);
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
        KafkaToKafkaPipeline mockKafkaToKafkaPipeline = new KafkaToKafkaPipeline(
                singletonList(mockSpanKeyExtractor),
                singletonList(new KafkaProducerWrapper("defaultTopic", "mockKafkaProducer", mockKafkaProducer, mockKafkaProducerMetrics)));
        when(mockSpanKeyExtractor.getRecords(FULLY_POPULATED_SPAN)).thenReturn(Collections.EMPTY_LIST);
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
        Record record = new Record(JSON_SPAN_STRING, "externalKafkaKey",
                Collections.singletonMap("mock-Topic", singletonList("extractedTopic")));
        when(mockSpanKeyExtractor.getRecords(any())).thenReturn(singletonList(record));
        KafkaToKafkaPipeline.logger = mockLogger;
        kafkaToKafkaPipeline.apply(null, FULLY_POPULATED_SPAN);
        KafkaToKafkaPipeline.logger = realLogger;
        verify(mockLogger).debug("Kafka Producer sending message: {},with key: {}  ", JSON_SPAN_STRING_WITH_FLATTENED_TAGS, "externalKafkaKey");
    }

    @Test
    public void testApplyWithoutTags() {
        Logger realLogger = KafkaToKafkaPipeline.logger;
        KafkaToKafkaPipeline.logger = mockLogger;
        Record record = new Record(JSON_SPAN_STRING_WITH_NO_TAGS, "externalKafkaKey",
                Collections.singletonMap("mock-Topic", Arrays.asList("extractedTopic")));
        when(mockSpanKeyExtractor.getRecords(any())).thenReturn(singletonList(record));
        kafkaToKafkaPipeline.apply(null, NO_TAGS_SPAN);
        KafkaToKafkaPipeline.logger = realLogger;
        verify(mockLogger).debug("Kafka Producer sending message: {},with key: {}  ", JSON_SPAN_STRING_WITH_NO_TAGS, "externalKafkaKey");
    }

    @Test
    public void testIfProducerTopicMappingIsNotPresent() {
        Logger realLogger = KafkaToKafkaPipeline.logger;
        Record record = new Record(JSON_SPAN_STRING, "externalKafkaKey",
                Collections.singletonMap("DefaultKafkaProducer-mock", singletonList("extractedTopic")));
        when(mockSpanKeyExtractor.getRecords(any())).thenReturn(singletonList(record));
        KafkaToKafkaPipeline.logger = mockLogger;
        kafkaToKafkaPipeline.apply(null, FULLY_POPULATED_SPAN);
        KafkaToKafkaPipeline.logger = realLogger;
        verify(mockLogger).error("Extractor skipped the span: {}, as no topics found for producer: {}", FULLY_POPULATED_SPAN, "mock-Topic");
    }


    @Test
    public void testIfProducerTopicMappingIsNull() {
        Logger realLogger = KafkaToKafkaPipeline.logger;
        Record record = new Record(JSON_SPAN_STRING, "externalKafkaKey", null);
        when(mockSpanKeyExtractor.getRecords(any())).thenReturn(singletonList(record));
        KafkaToKafkaPipeline.logger = mockLogger;
        kafkaToKafkaPipeline.apply(null, FULLY_POPULATED_SPAN);
        KafkaToKafkaPipeline.logger = realLogger;
        verify(mockLogger).error("Extractor skipped the span: {}, as no topics found for producer: {}", FULLY_POPULATED_SPAN, "mock-Topic");
    }

    @Test
    public void testFactoryCreateProducerRecord() {
        final ProducerRecord<String, String> producerRecord = KafkaToKafkaPipeline.factory.createProducerRecord(TOPIC, KEY, VALUE);
        assertEquals(KEY, producerRecord.key());
        assertEquals(VALUE, producerRecord.value());
    }
}