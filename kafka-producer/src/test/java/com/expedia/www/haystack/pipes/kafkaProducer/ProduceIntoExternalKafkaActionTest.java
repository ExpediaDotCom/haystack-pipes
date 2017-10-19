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

import com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.Factory;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.DEBUG_MSG;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.ERROR;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.ERROR_MSG;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.KAFKA_PRODUCER_POST;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.REQUEST;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.factory;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.kafkaProducer;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaAction.logger;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProduceIntoExternalKafkaActionTest {
    private final static Random RANDOM = new Random();
    private final static String KEY = RANDOM.nextLong() + "KEY";
    private final static String VALUE = RANDOM.nextLong() + "VALUE";
    private final static String TOPIC = RANDOM.nextLong() + "TOPIC";
    private final static int PARTITION = RANDOM.nextInt(Integer.MAX_VALUE);
    private final static long BASE_OFFSET = RANDOM.nextInt(Integer.MAX_VALUE);
    private final static long RELATIVE_OFFSET = RANDOM.nextInt(Integer.MAX_VALUE);
    private final static long TIMESTAMP = System.currentTimeMillis();
    private final static long CHECKSUM = RANDOM.nextLong();
    private final static int SERIALIZED_KEY_SIZE = RANDOM.nextInt(Integer.MAX_VALUE);
    private final static int SERIALIZED_VALUE_SIZE = RANDOM.nextInt(Integer.MAX_VALUE);
    private final static TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
    private final static RecordMetadata RECORD_METADATA = new RecordMetadata(TOPIC_PARTITION, BASE_OFFSET,
            RELATIVE_OFFSET, TIMESTAMP, CHECKSUM, SERIALIZED_KEY_SIZE, SERIALIZED_VALUE_SIZE);

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
    private Future<RecordMetadata> mockRecordMetadataFuture;

    @Mock
    private Stopwatch mockStopwatch;

    private ProduceIntoExternalKafkaAction produceIntoExternalKafkaAction;

    @Before
    public void setUp() {
        injectMockAndSaveRealObjects();
        produceIntoExternalKafkaAction = new ProduceIntoExternalKafkaAction();
    }

    @After
    public void tearDown() {
        restoreRealObjects();
        resetCounters();
        verifyNoMoreInteractions(mockTimer, mockLogger, mockFactory, mockKafkaProducer, mockProducerRecord,
                mockRecordMetadataFuture, mockStopwatch);
    }

    private void injectMockAndSaveRealObjects() {
        saveRealAndInjectMockTimer();
        saveRealAndInjectMockLogger();
        saveRealAndInjectMockFactory();
        saveRealAndInjectMockProducer();
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
        KAFKA_PRODUCER_POST = realTimer;
        logger = realLogger;
        factory = realFactory;
        kafkaProducer = realKafkaProducer;
    }

    private void resetCounters() {
        REQUEST.increment(-((long) REQUEST.getValue()));
        ERROR.increment(-((long) ERROR.getValue()));
    }

    @Test
    public void testApplySuccessDoNotWaitForResponse() throws ExecutionException, InterruptedException {
        whensForTestApplySuccess(false, false);

        produceIntoExternalKafkaAction.apply(KEY, VALUE);

        verifyCounters(0);
        verifiesForTestApplySuccess(false, true);
    }

    @Test
    public void testApplySuccessWaitForResponseDebugEnabled() throws ExecutionException, InterruptedException {
        whensForTestApplySuccess(true, true);

        produceIntoExternalKafkaAction.apply(KEY, VALUE);

        verifyCounters(0);
        verifiesForTestApplySuccess(true, true);
    }

    @Test
    public void testApplySuccessWaitForResponseDebugDisabled() throws ExecutionException, InterruptedException {
        whensForTestApplySuccess(true, false);

        produceIntoExternalKafkaAction.apply(KEY, VALUE);

        verifyCounters(0);
        verifiesForTestApplySuccess(true, false);
    }

    private void whensForTestApplySuccess(boolean waitForResponse, boolean isDebugEnabled)
            throws InterruptedException, ExecutionException {
        whensForTestApply();
        if(waitForResponse) {
            when(mockLogger.isDebugEnabled()).thenReturn(isDebugEnabled);
            when(mockRecordMetadataFuture.get()).thenReturn(RECORD_METADATA);
        } else {
            putWaitForResponseFalseIntoEnvironmentVariables();
        }
    }

    private void verifiesForTestApplySuccess(boolean waitForResponse, boolean isDebugEnabled)
            throws InterruptedException, ExecutionException {
        verifiesForTestApply(waitForResponse);
        if(waitForResponse) {
            verify(mockLogger).isDebugEnabled();
            if (isDebugEnabled) {
                verify(mockLogger).debug(String.format(DEBUG_MSG, VALUE, PARTITION));
            }
        }
    }

    @Test
    public void testApplyExecutionException() throws ExecutionException, InterruptedException {
        final Exception executionException = new ExecutionException("Test", null);
        whensForTestApply();
        when(mockRecordMetadataFuture.get()).thenThrow(executionException);

        produceIntoExternalKafkaAction.apply(KEY, VALUE);

        verifyCounters(1);
        verifiesForTestApply(true);
        verify(mockLogger).error(ERROR_MSG, VALUE, executionException);
    }

    private void whensForTestApply() {
        when(mockTimer.start()).thenReturn(mockStopwatch);
        when(mockFactory.createProducerRecord(anyString(), anyString())).thenReturn(mockProducerRecord);
        when(mockKafkaProducer.send(Matchers.any())).thenReturn(mockRecordMetadataFuture);
    }

    private void verifiesForTestApply(boolean waitForResponse) throws InterruptedException, ExecutionException {
        verify(mockTimer).start();
        verify(mockFactory).createProducerRecord(KEY, VALUE);
        verify(mockKafkaProducer).send(mockProducerRecord);
        if(waitForResponse) {
            verify(mockRecordMetadataFuture).get();
        }
        verify(mockStopwatch).stop();
    }

    private void verifyCounters(long errorCount) {
        assertEquals(1L, REQUEST.getValue());
        assertEquals(errorCount, ERROR.getValue());
    }

    @Test
    public void testFactoryCreateProducerRecord() {
        final ProducerRecord<String, String> producerRecord = realFactory.createProducerRecord(KEY, VALUE);

        assertEquals(KEY, producerRecord.key());
        assertEquals(VALUE, producerRecord.value());
    }

    private void putWaitForResponseFalseIntoEnvironmentVariables() {
        try {
            final Map<String,String> unmodifiableEnv = System.getenv();
            final Class<?> cl = unmodifiableEnv.getClass();

            // It is not intended that environment variables be changed after the JVM starts, thus reflection
            @SuppressWarnings("JavaReflectionMemberAccess")
            final Field field = cl.getDeclaredField("m");
            field.setAccessible(true);

            @SuppressWarnings("unchecked")
            final Map<String,String> modifiableEnv = (Map<String,String>) field.get(unmodifiableEnv);
            modifiableEnv.put("HAYSTACK_EXTERNALKAFKA_WAITFORRESPONSE", Boolean.FALSE.toString());
            field.setAccessible(false);
            ProduceIntoExternalKafkaAction.EKCP.reload();
        } catch(Exception e) {
            throw new RuntimeException("Unable to access writable environment variable map.");
        }
    }
}
