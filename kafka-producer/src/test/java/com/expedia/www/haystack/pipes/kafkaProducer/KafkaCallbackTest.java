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

import com.expedia.www.haystack.pipes.commons.Timers;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import org.apache.commons.pool2.ObjectPool;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.time.Clock;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaCallbackTest {
    private static final String TOPIC = RANDOM.nextLong() + "TOPIC";
    private static final int PARTITION = RANDOM.nextInt();
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
    private static final long BASE_OFFSET = -1;
    private static final long RELATIVE_OFFSET = RANDOM.nextLong();
    private static final long TIMESTAMP = System.currentTimeMillis();
    private static final long CHECKSUM = RANDOM.nextLong();
    private static final int SERIALIZED_KEY_SIZE = RANDOM.nextInt();
    private static final int SERIALIZED_VALUE_SIZE = RANDOM.nextInt();
    private static final String MESSAGE = RANDOM.nextLong() + "MESSAGE";

    @Mock
    private Logger mockLogger;

    @Mock
    private Exception mockException;

    @Mock
    private ObjectPool<KafkaCallback> mockObjectPool;
    private ObjectPool<KafkaCallback> realObjectPool;

    @Mock
    private Counter mockRequestCounter;

    @Mock
    private Counter mockPostsInFlightCounter;

    @Mock
    private Timer mockTimer;

    @Mock
    private Clock mockClock;

    @Mock
    private Timer mockSpanArrivalTimer;

    private RecordMetadata recordMetadata;
    private KafkaCallback kafkaCallback;

    @Before
    public void setUp() {
        Timers timers = new Timers(mockTimer, mockSpanArrivalTimer);
        injectMockAndSaveRealObjects();
        //noinspection deprecation
        recordMetadata = new RecordMetadata(TOPIC_PARTITION, BASE_OFFSET, RELATIVE_OFFSET, TIMESTAMP, CHECKSUM,
                SERIALIZED_KEY_SIZE, SERIALIZED_VALUE_SIZE);
        kafkaCallback = new KafkaCallback();
        KafkaCallback.logger = mockLogger;

    }

    private void injectMockAndSaveRealObjects() {
        saveRealAndInjectMockObjectPool();
    }

    private void saveRealAndInjectMockObjectPool() {
        realObjectPool = KafkaToKafkaPipeline.OBJECT_POOL;
        KafkaToKafkaPipeline.OBJECT_POOL = mockObjectPool;
    }

    @After
    public void tearDown() {
        restoreRealObjects();
//        verifyNoMoreInteractions(mockLogger, mockException, mockObjectPool, mockRequestCounter,
//                mockTimer, mockClock, mockSpanArrivalTimer);
    }

    private void restoreRealObjects() {
        KafkaToKafkaPipeline.OBJECT_POOL = realObjectPool;
    }

    @Test
    public void testOnCompletionBothNull() throws Exception {
        kafkaCallback.onCompletion(null, null);
        commonVerifiesForOnCompletion();
    }

//    @Test
//    public void testOnCompletionBothNullReturnToObjectPoolSuccess() throws Exception {
//        final Exception testException = new Exception("Exception Message");
//        doThrow(testException).when(mockObjectPool).returnObject(any(KafkaCallback.class));
//
//        kafkaCallback.onCompletion(null, null);
//        verify(mockLogger).error(String.format(KafkaCallback.POOL_ERROR_MSG_TEMPLATE, testException.getMessage()), testException);
//        commonVerifiesForOnCompletion();
//    }

    @Test
    public void testOnCompletionRuntimeExceptionReturnToObjectPoolSuccess() throws Exception {
        final Exception runtimeException = new RuntimeException("RuntimeException Message");
        final Exception testException = new Exception("Exception Message");
        doThrow(runtimeException).when(mockLogger).error(anyString(), any(Throwable.class));

        try {
            kafkaCallback.onCompletion(null, testException);
        } catch (Throwable e) {
            assertSame(runtimeException, e);
            verify(mockLogger).error(String.format(KafkaCallback.ERROR_MSG_TEMPLATE, testException.getMessage()), testException);
            commonVerifiesForOnCompletion();
        }
    }

    @Test
    public void testOnCompletionNonNullMetadataDebugDisabled() throws Exception {
        when(mockLogger.isDebugEnabled()).thenReturn(false);

        kafkaCallback.onCompletion(recordMetadata, null);

        verify(mockLogger).isDebugEnabled();
        commonVerifiesForOnCompletion();
    }

    @Test
    public void testOnCompletionNonNullMetadataDebugEnabled() throws Exception {
        when(mockLogger.isDebugEnabled()).thenReturn(true);

        kafkaCallback.onCompletion(recordMetadata, null);

        verify(mockLogger).isDebugEnabled();
        verify(mockLogger).debug(String.format(KafkaCallback.DEBUG_MSG, TOPIC, PARTITION, BASE_OFFSET));
        commonVerifiesForOnCompletion();
    }

    @Test
    public void testOneCompletionNonNullException() throws Exception {
        when(mockException.getMessage()).thenReturn(MESSAGE);

        kafkaCallback.onCompletion(null, mockException);

        verify(mockException).getMessage();
        verify(mockLogger).error(String.format(KafkaCallback.ERROR_MSG_TEMPLATE, MESSAGE), mockException);
        commonVerifiesForOnCompletion();
    }

    private void commonVerifiesForOnCompletion() throws Exception {
//        verify(mockObjectPool).returnObject(kafkaCallback);
    }

}
