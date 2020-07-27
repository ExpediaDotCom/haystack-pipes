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
import com.expedia.www.haystack.pipes.commons.key.extractor.config.SpanKeyExtractorConfigProvider;
import com.expedia.www.haystack.pipes.kafka.KafkaToKafkaPipeline.Factory;
import com.netflix.servo.monitor.Stopwatch;
import org.apache.commons.pool2.ObjectPool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Map;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_FLATTENED_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_NO_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.NO_TAGS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.kafka.KafkaToKafkaPipeline.POSTS_IN_FLIGHT_COUNTER_INDEX;
import static com.expedia.www.haystack.pipes.kafka.KafkaToKafkaPipeline.TOPIC_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaToKafkaPipelineTest {
    private static final String TOPIC = RANDOM.nextLong() + "TOPIC";
    private static final String KEY = RANDOM.nextLong() + "KEY";
    private static final String VALUE = RANDOM.nextLong() + "VALUE";
    private static final String BROKERS = RANDOM.nextLong() + "BROKERS";
    private static final int PORT = RANDOM.nextInt();

    @Mock
    private Factory mockFactory;
    @Mock
    private TimersAndCounters mockTimersAndCounters;
    @Mock
    private Logger mockLogger;
    @Mock
    private ExternalKafkaConfigurationProvider mockKafkaConfigurationProvider;
    @Mock
    private SpanKeyExtractorConfigProvider mockSpanKeyExtractorConfigProvider;
    @Mock
    private Stopwatch mockStopwatch;
    @Mock
    private KafkaProducer<String, String> mockKafkaProducer;
    @Mock
    private ProducerRecord<String, String> mockProducerRecord;
    @Mock
    private ObjectPool<KafkaCallback> mockObjectPool;
    @Mock
    private Map<String, Object> mockMap;

    private KafkaToKafkaPipeline produceIntoExternalKafkaAction;
    private Factory realFactory;

    @Before
    public void setUp() {
        whensForConstructor();
        produceIntoExternalKafkaAction = new KafkaToKafkaPipeline(
                mockFactory, mockTimersAndCounters, mockLogger, mockKafkaConfigurationProvider, mockSpanKeyExtractorConfigProvider);
        realFactory = new Factory();
    }

    private void whensForConstructor() {
        when(mockKafkaConfigurationProvider.getConfigurationMap()).thenReturn(mockMap);
        when(mockFactory.createKafkaProducer(anyMapOf(String.class, Object.class))).thenReturn(mockKafkaProducer);
        when(mockKafkaConfigurationProvider.totopic()).thenReturn(TOPIC);
        when(mockKafkaConfigurationProvider.brokers()).thenReturn(BROKERS);
        when(mockKafkaConfigurationProvider.port()).thenReturn(PORT);
        when(mockSpanKeyExtractorConfigProvider.loadAndGetSpanExtractor()).thenReturn(null);
    }

    @After
    public void tearDown() {
//        verifiesForConstructor();
//        verifyNoMoreInteractions(mockFactory, mockTimersAndCounters, mockLogger, mockKafkaConfigurationProvider, mockSpanKeyExtractorConfigProvider);
//        verifyNoMoreInteractions(mockStopwatch, mockKafkaProducer, mockProducerRecord, mockObjectPool, mockMap);
    }

    private void verifiesForConstructor() {
        verify(mockKafkaConfigurationProvider).getConfigurationMap();
        verify(mockFactory).createKafkaProducer(mockMap);
        verify(mockKafkaConfigurationProvider).totopic();
        verify(mockKafkaConfigurationProvider).brokers();
        verify(mockKafkaConfigurationProvider).port();
        verify(mockLogger).info(String.format(TOPIC_MESSAGE, BROKERS, PORT, TOPIC));
        verify(mockSpanKeyExtractorConfigProvider).loadAndGetSpanExtractor();

    }

    @Test
    public void testApplySuccessWithTags() {
        testApplySuccess(FULLY_POPULATED_SPAN, JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
    }

    @Test
    public void testApplySuccessWithoutTags() {
        testApplySuccess(NO_TAGS_SPAN, JSON_SPAN_STRING_WITH_NO_TAGS);
    }

    private void testApplySuccess(Span span, String jsonSpanString) {
        whensForTestApply();

        produceIntoExternalKafkaAction.apply(KEY, span);

        verify(mockTimersAndCounters).incrementRequestCounter();
        verifiesForTestApply(jsonSpanString);
    }

//    @Test(expected = OutOfMemoryError.class)
//    public void testApplyOutOfMemoryError() {
//        whensForTestApply();
//        when(mockKafkaProducer.send(any(), any())).thenThrow(new OutOfMemoryError());
//
//        try {
//            produceIntoExternalKafkaAction.apply(KEY, FULLY_POPULATED_SPAN);
//        } finally {
//            verify(mockTimersAndCounters).incrementRequestCounter();
//            verifiesForTestApply(JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
//        }
//
//    }

    private void whensForTestApply() {
        when(mockTimersAndCounters.startTimer()).thenReturn(mockStopwatch);
        when(mockFactory.createProducerRecord(anyString(), anyString(), anyString())).thenReturn(mockProducerRecord);
    }

    private void verifiesForTestApply(String jsonSpanString) {
        verify(mockTimersAndCounters).startTimer();
        verify(mockFactory).createProducerRecord(TOPIC, KEY, jsonSpanString);
        // TODO verify below without any() when the ProduceIntoExternalKafkaCallback object is returned by a factory
        verify(mockKafkaProducer).send(eq(mockProducerRecord), any(KafkaCallback.class));
        verify(mockStopwatch).stop();
        verify(mockTimersAndCounters).incrementCounter(POSTS_IN_FLIGHT_COUNTER_INDEX);
    }

    @Test
    public void testFactoryCreateProducerRecord() {
        final ProducerRecord<String, String> producerRecord = realFactory.createProducerRecord(TOPIC, KEY, VALUE);

        assertEquals(KEY, producerRecord.key());
        assertEquals(VALUE, producerRecord.value());
    }

}
