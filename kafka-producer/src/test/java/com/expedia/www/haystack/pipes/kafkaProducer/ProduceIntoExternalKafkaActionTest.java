/*
 * Copyright 2017 Expedia, Inc.
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
import com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.Factory;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Random;

import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.ERROR_MSG;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.KAFKA_PRODUCER_POST;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.POSTS_IN_FLIGHT;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.REQUEST;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.factory;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.kafkaProducer;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.logger;
import static com.expedia.www.haystack.pipes.kafkaProducer.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.kafkaProducer.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_BOGUS_TAGS;
import static com.expedia.www.haystack.pipes.kafkaProducer.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_EMPTY_TAGS;
import static com.expedia.www.haystack.pipes.kafkaProducer.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_FLATTENED_TAGS;
import static com.expedia.www.haystack.pipes.kafkaProducer.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_NO_TAGS;
import static com.expedia.www.haystack.pipes.kafkaProducer.TestConstantsAndCommonCode.NO_TAGS_SPAN;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProduceIntoExternalKafkaActionTest {
    private final static Random RANDOM = new Random();
    private final static String KEY = RANDOM.nextLong() + "KEY";
    private final static String VALUE = RANDOM.nextLong() + "VALUE";

    @Mock
    private Counter mockRequestCounter;
    private Counter realRequestCounter;
    @Mock
    private Counter mockPostsInFlightCounter;
    private Counter realPostsInFlightCounter;
    @Mock
    private Timer mockTimer;
    private Timer realTimer;
    @Mock
    private Logger mockLogger;
    private Logger realLogger;
    @Mock
    private Factory mockFactory;
    private Factory realFactory;
    @Mock
    private KafkaProducer<String, String> mockKafkaProducer;
    private KafkaProducer<String, String> realKafkaProducer;

    @Mock
    private ProducerRecord<String, String> mockProducerRecord;

    @Mock
    private Stopwatch mockStopwatch;

    private ProduceIntoExternalKafkaAction produceIntoExternalKafkaAction;

    @Before
    public void setUp() {
        injectMocksAndSaveRealObjects();
        produceIntoExternalKafkaAction = new ProduceIntoExternalKafkaAction();
    }

    @After
    public void tearDown() {
        restoreRealObjects();
        verifyNoMoreInteractions(mockRequestCounter, mockPostsInFlightCounter, mockTimer, mockLogger, mockFactory,
                mockKafkaProducer, mockProducerRecord, mockStopwatch);
    }

    private void injectMocksAndSaveRealObjects() {
        saveRealAndInjectMockRequestCounter();
        saveRealAndInjectMockPostsInFlightCounter();
        saveRealAndInjectMockTimer();
        saveRealAndInjectMockLogger();
        saveRealAndInjectMockFactory();
        saveRealAndInjectMockProducer();
    }

    private void saveRealAndInjectMockRequestCounter() {
        realRequestCounter = REQUEST;
        REQUEST = mockRequestCounter;
    }

    private void saveRealAndInjectMockPostsInFlightCounter() {
        realPostsInFlightCounter = POSTS_IN_FLIGHT;
        POSTS_IN_FLIGHT = mockPostsInFlightCounter;
    }

    private void saveRealAndInjectMockTimer() {
        realTimer = KAFKA_PRODUCER_POST;
        KAFKA_PRODUCER_POST = mockTimer;
    }

    private void saveRealAndInjectMockLogger() {
        realLogger = logger;
        logger = mockLogger;
    }

    private void saveRealAndInjectMockFactory() {
        realFactory = factory;
        factory = mockFactory;
    }

    private void saveRealAndInjectMockProducer() {
        realKafkaProducer = kafkaProducer;
        kafkaProducer = mockKafkaProducer;
    }

    private void restoreRealObjects() {
        REQUEST = realRequestCounter;
        POSTS_IN_FLIGHT = realPostsInFlightCounter;
        KAFKA_PRODUCER_POST = realTimer;
        logger = realLogger;
        factory = realFactory;
        kafkaProducer = realKafkaProducer;
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

        verify(mockRequestCounter).increment();
        verifiesForTestApply(jsonSpanString);
    }

    @Test
    public void testApplyNullPointerException() {
        whensForTestApply();

        produceIntoExternalKafkaAction.apply(KEY, null);

        verify(mockRequestCounter).increment();
        verify(mockTimer).start();
        verify(mockLogger).error(eq(String.format(ERROR_MSG, "", null)), any(NullPointerException.class));
        verify(mockStopwatch).stop();
    }

    @Test(expected = OutOfMemoryError.class)
    public void testApplyOutOfMemoryError() {
        whensForTestApply();
        when(mockKafkaProducer.send(any(), any())).thenThrow(new OutOfMemoryError());

        try {
            produceIntoExternalKafkaAction.apply(KEY, FULLY_POPULATED_SPAN);
        } finally {
            verify(mockRequestCounter).increment();
            verifiesForTestApply(JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
        }

    }

    private void whensForTestApply() {
        when(mockTimer.start()).thenReturn(mockStopwatch);
        when(mockFactory.createProducerRecord(anyString(), anyString())).thenReturn(mockProducerRecord);
    }

    private void verifiesForTestApply(String jsonSpanString) {
        verify(mockTimer).start();
        verify(mockFactory).createProducerRecord(KEY, jsonSpanString);
        // TODO verify below without any() when the ProduceIntoExternalKafkaCallback object is returned by a factory
        verify(mockKafkaProducer).send(eq(mockProducerRecord), any(ProduceIntoExternalKafkaCallback.class));
        verify(mockStopwatch).stop();
        verify(mockPostsInFlightCounter).increment();
    }

    @Test
    public void testFactoryCreateProducerRecord() {
        final ProducerRecord<String, String> producerRecord = realFactory.createProducerRecord(KEY, VALUE);

        assertEquals(KEY, producerRecord.key());
        assertEquals(VALUE, producerRecord.value());
    }

    @Test
    public void testFlattenTagsWithBogusTag() {
        final String flattenedTags = ProduceIntoExternalKafkaAction.flattenTags(JSON_SPAN_STRING_WITH_BOGUS_TAGS);
        assertEquals(JSON_SPAN_STRING_WITH_EMPTY_TAGS, flattenedTags);
    }
}
