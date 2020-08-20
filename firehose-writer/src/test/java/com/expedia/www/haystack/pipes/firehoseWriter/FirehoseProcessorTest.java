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
package com.expedia.www.haystack.pipes.firehoseWriter;

import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.config.FirehoseConfig;
import com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.Factory;
import com.netflix.servo.monitor.Stopwatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.Semaphore;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.STARTUP_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseProcessorTest {
    private static final String KEY = RANDOM.nextLong() + "KEY";
    private static final String STREAM_NAME = RANDOM.nextLong() + "STREAM_NAME";
    private static final Integer INITIAL_RETRY_SLEEP = 100;
    private static final Integer MAX_RETRY_SLEEP = 1000;
    private static final Integer MAX_PARALLELISM_PER_SHARD = RANDOM.nextInt(Byte.MAX_VALUE);
    private static final InterruptedException INTERRUPTED_EXCEPTION = new InterruptedException();

    @Mock
    private Batch mockBatch;
    @Mock
    private Factory mockFactory;
    @Mock
    private FirehoseConfig mockFirehoseConfigurationProvider;
    @Mock
    private FirehoseTimersAndCounters mockFirehoseCountersAndTimer;
    @Mock
    private FirehoseProcessor mockFirehoseProcessor;
    @Mock
    private List<Record> mockRecordList;
    @Mock
    private Logger mockLogger;
    @Mock
    private S3Sender mockS3Sender;
    @Mock
    private RetryCalculator mockRetryCalculator;
    @Mock
    private Semaphore mockSemaphore;
    @Mock
    private Callback mockCallback;
    @Mock
    private Sleeper mockSleeper;
    @Mock
    private Stopwatch mockStopwatch;
    @Mock
    private Thread mockThread;

    private FirehoseProcessor firehoseProcessor;
    private Factory factory;
    private Sleeper sleeper;
    private int wantedNumberOfInvocationsStreamName = 2;
    private int numberOfTimesConstructorWasCalled = 1;

    @Before
    public void setUp() {
        when(mockFirehoseConfigurationProvider.getStreamName()).thenReturn(STREAM_NAME);
        when(mockFirehoseConfigurationProvider.getMaxParallelISMPerShard()).thenReturn(MAX_PARALLELISM_PER_SHARD);
        when(mockFactory.createSemaphore(anyInt())).thenReturn(mockSemaphore);
        firehoseProcessor = new FirehoseProcessor(mockLogger, mockFirehoseCountersAndTimer, () -> mockBatch,
                mockFactory, mockFirehoseConfigurationProvider, mockS3Sender);
        factory = new Factory();
        sleeper = factory.createSleeper();
    }

    @After
    public void tearDown() {
        verify(mockFirehoseConfigurationProvider, times(wantedNumberOfInvocationsStreamName)).getStreamName();
        verify(mockFirehoseConfigurationProvider, times(numberOfTimesConstructorWasCalled)).getMaxParallelISMPerShard();
        verify(mockFactory, times(numberOfTimesConstructorWasCalled)).createSemaphore(MAX_PARALLELISM_PER_SHARD);
        verify(mockLogger, times(numberOfTimesConstructorWasCalled)).info(String.format(STARTUP_MESSAGE, STREAM_NAME));
        verifyNoMoreInteractions(mockBatch);
        verifyNoMoreInteractions(mockFactory);
        verifyNoMoreInteractions(mockFirehoseConfigurationProvider);
        verifyNoMoreInteractions(mockFirehoseCountersAndTimer);
        verifyNoMoreInteractions(mockFirehoseProcessor);
        verifyNoMoreInteractions(mockRecordList);
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockS3Sender);
        verifyNoMoreInteractions(mockRetryCalculator);
        verifyNoMoreInteractions(mockSemaphore);
        verifyNoMoreInteractions(mockSleeper);
        verifyNoMoreInteractions(mockStopwatch);
        verifyNoMoreInteractions(mockThread);
    }

    @Test
    public void testApplyEmptyList() {
        wantedNumberOfInvocationsStreamName = 1;
        when(mockBatch.getRecordList(any(Span.class))).thenReturn(mockRecordList);
        when(mockRecordList.isEmpty()).thenReturn(true);

        firehoseProcessor.process(consumerRecord(KEY, FULLY_POPULATED_SPAN));

        commonVerifiesForTestApply(0);
        verify(mockFirehoseCountersAndTimer).recordSpanArrivalDelta(FULLY_POPULATED_SPAN);
    }

    @Test
    public void testApplyHappyPath() throws InterruptedException {
        when(mockFactory.createCallback(anyObject(), anyObject())).thenReturn(mockCallback);

        wantedNumberOfInvocationsStreamName = 1;
        commonWhensForTestApply();

        firehoseProcessor.process(consumerRecord(KEY, FULLY_POPULATED_SPAN));

        commonVerifiesForTestApply(1);
        commonVerifiesForTestApplyNotEmpty();
        verify(mockFirehoseCountersAndTimer).recordSpanArrivalDelta(FULLY_POPULATED_SPAN);
        verify(mockS3Sender).sendRecordsToS3(mockRecordList, mockRetryCalculator, mockSleeper, 0, mockCallback);
        verify(mockSemaphore).acquire();
        verify(mockFactory, times(numberOfTimesConstructorWasCalled)).createCallback(anyObject(), anyObject());
    }

    @Test
    public void testInterruptedException() throws InterruptedException {
        numberOfTimesConstructorWasCalled = 2;
        when(mockFactory.createSemaphore(anyInt())).thenReturn(mockSemaphore);
        when(mockFactory.currentThread()).thenReturn(mockThread);
        firehoseProcessor = new FirehoseProcessor(mockLogger, mockFirehoseCountersAndTimer, () -> mockBatch,
                mockFactory, mockFirehoseConfigurationProvider, mockS3Sender);
        commonWhensForTestApply();
        doThrow(INTERRUPTED_EXCEPTION).when(mockSemaphore).acquire();

        firehoseProcessor.process(consumerRecord(KEY, FULLY_POPULATED_SPAN));

        commonVerifiesForTestApply(0);
        verify(mockFactory).currentThread();
        verify(mockSemaphore).acquire();
        verify(mockFirehoseCountersAndTimer).recordSpanArrivalDelta(FULLY_POPULATED_SPAN);
        verify(mockThread).interrupt();
    }

    @Test
    public void testClose() {
        wantedNumberOfInvocationsStreamName = 1;
        when(mockRecordList.isEmpty()).thenReturn(true);
        when(mockBatch.getRecordListForShutdown()).thenReturn(mockRecordList);

        firehoseProcessor.close();
    }

    private void commonWhensForTestApply() {
        when(mockBatch.getRecordList(any(Span.class))).thenReturn(mockRecordList);
        when(mockRecordList.isEmpty()).thenReturn(false);
        when(mockFirehoseCountersAndTimer.startTimer()).thenReturn(mockStopwatch);
        when(mockFactory.createSleeper()).thenReturn(mockSleeper);
        when(mockFirehoseConfigurationProvider.getInitialRetrySleep()).thenReturn(INITIAL_RETRY_SLEEP);
        when(mockFirehoseConfigurationProvider.getMaxRetrySleep()).thenReturn(MAX_RETRY_SLEEP);
        when(mockFactory.createRetryCalculator(anyInt(), anyInt())).thenReturn(mockRetryCalculator);
    }

    private void commonVerifiesForTestApply(int configurationTimes) {
        verify(mockFirehoseCountersAndTimer).incrementRequestCounter();
        verify(mockBatch).getRecordList(FULLY_POPULATED_SPAN);
        //noinspection ResultOfMethodCallIgnored
        verify(mockRecordList).isEmpty();
        verify(mockFirehoseConfigurationProvider, times(configurationTimes)).getMaxRetrySleep();
        verify(mockFactory, times(configurationTimes)).createRetryCalculator(INITIAL_RETRY_SLEEP, MAX_RETRY_SLEEP);
    }

    private void commonVerifiesForTestApplyNotEmpty() {
        verify(mockFactory).createSleeper();
        verify(mockFirehoseConfigurationProvider).getInitialRetrySleep();
    }

    @Test
    public void testSleeperSleep() throws InterruptedException {
        wantedNumberOfInvocationsStreamName = 1;
        sleeper.sleep(0);
    }

    @Test
    public void testFactoryCreateRetryCalculator() {
        wantedNumberOfInvocationsStreamName = 1;
        final RetryCalculator retryCalculator = factory.createRetryCalculator(INITIAL_RETRY_SLEEP, MAX_RETRY_SLEEP);

        assertEquals(BigInteger.valueOf(INITIAL_RETRY_SLEEP), retryCalculator.initialRetrySleep);
        assertEquals(BigInteger.valueOf(MAX_RETRY_SLEEP), retryCalculator.maxRetrySleep);
    }

    @Test
    public void testFactoryGetRuntime() {
        wantedNumberOfInvocationsStreamName = 1;
        assertSame(Runtime.getRuntime(), factory.getRuntime());
    }

    @Test
    public void testFactoryCreateShutdownHookAndShutdownHookClass() {
        wantedNumberOfInvocationsStreamName = 1;
        final Thread shutdownHook = factory.createShutdownHook(mockFirehoseProcessor);
        shutdownHook.run();

        verify(mockFirehoseProcessor).close();
    }

    @Test
    public void testFactoryCurrentThread() {
        wantedNumberOfInvocationsStreamName = 1;
        final Thread thread = factory.currentThread();

        assertSame(Thread.currentThread(), thread);
    }

    @Test
    public void testCreateSemaphore() {
        wantedNumberOfInvocationsStreamName = 1;
        final Semaphore semaphore = factory.createSemaphore(MAX_PARALLELISM_PER_SHARD);

        assertEquals((long) MAX_PARALLELISM_PER_SHARD, semaphore.availablePermits());
    }


    private ConsumerRecord<String, Span> consumerRecord(String key, Span fullyPopulatedSpan) {
        return new ConsumerRecord<>("topic", 1, 1, key, fullyPopulatedSpan);
    }
}
