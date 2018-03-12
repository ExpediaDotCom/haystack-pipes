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

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.Factory;
import com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.Sleeper;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.PUT_RECORD_BATCH_ERROR_MSG;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.PUT_RECORD_BATCH_WARN_MSG;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.SLEEP_STEP;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.STARTUP_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseProcessorTest {
    private static final String KEY = RANDOM.nextLong() + "KEY";
    private static final int RETRY_COUNT = 1 + RANDOM.nextInt(Byte.MAX_VALUE);
    private static final String STREAM_NAME = RANDOM.nextLong() + "STREAM_NAME";
    private static final long TIMESTAMP = System.currentTimeMillis();

    @Mock
    private Logger mockLogger;
    @Mock
    private Counters mockCounters;
    @Mock
    private Timer mockTimer;
    @Mock
    private Batch mockBatch;
    @Mock
    private AmazonKinesisFirehose mockAmazonKinesisFirehose;
    @Mock
    private Factory mockFactory;
    @Mock
    private FirehoseConfigurationProvider mockFirehoseConfigurationProvider;

    @Mock
    private List<Record> mockRecordList;
    @Mock
    private PutRecordBatchRequest mockRequest;
    @Mock
    private Stopwatch mockStopwatch;
    @Mock
    private PutRecordBatchResult mockResult;
    @Mock
    private Sleeper mockSleeper;
    @Mock
    private ProcessorContext mockProcessorContext;
    @Mock
    private FirehoseProcessor mockFirehoseProcessor;
    @Mock
    private Thread mockThread;
    @Mock
    private Runtime mockRuntime;

    private FirehoseProcessor firehoseProcessor;
    private Factory factory;
    private Sleeper sleeper;
    private int wantedNumberOfInvocationsStreamName = 2;

    @Before
    public void setUp() {
        when(mockFirehoseConfigurationProvider.streamname()).thenReturn(STREAM_NAME);
        firehoseProcessor = new FirehoseProcessor(mockLogger, mockCounters, mockTimer, mockBatch, mockAmazonKinesisFirehose,
                mockFactory, mockFirehoseConfigurationProvider);
        factory = new Factory();
        sleeper = factory.createSleeper();
    }

    @After
    public void tearDown() {
        verify(mockFirehoseConfigurationProvider, times(wantedNumberOfInvocationsStreamName)).streamname();
        verify(mockLogger).info(String.format(STARTUP_MESSAGE, STREAM_NAME));
        verifyNoMoreInteractions(mockLogger, mockCounters, mockTimer, mockBatch, mockAmazonKinesisFirehose,
                mockFactory, mockFirehoseConfigurationProvider, mockProcessorContext);
        verifyNoMoreInteractions(mockRecordList, mockRequest, mockStopwatch, mockResult, mockSleeper,
                mockFirehoseProcessor, mockThread, mockRuntime);
    }

    @Test
    public void testApplyEmptyList() {
        wantedNumberOfInvocationsStreamName = 1;
        when(mockBatch.getRecordList(any(Span.class))).thenReturn(mockRecordList);
        when(mockRecordList.isEmpty()).thenReturn(true);

        firehoseProcessor.process(KEY, FULLY_POPULATED_SPAN);

        commonVerifiesForTestApply(1);
    }

    @Test
    public void testApplyHappyPath() throws InterruptedException {
        commonWhensForTestApply(RETRY_COUNT);
        when(mockAmazonKinesisFirehose.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(mockResult);

        firehoseProcessor.process(KEY, FULLY_POPULATED_SPAN);

        commonVerifiesForTestApply(1);
        commonVerifiesForTestApplyNotEmpty(1, 1);
        verify(mockCounters).countSuccessesAndFailures(mockRequest, mockResult);
    }

    @Test
    public void testClose() throws InterruptedException {
        when(mockBatch.getRecordListForShutdown()).thenReturn(mockRecordList);
        commonWhensForTestApply(RETRY_COUNT);
        when(mockAmazonKinesisFirehose.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(mockResult);

        firehoseProcessor.close();

        commonVerifiesForTestApply(0);
        commonVerifiesForTestApplyNotEmpty(1, 1);
        verify(mockCounters).countSuccessesAndFailures(mockRequest, mockResult);
        verify(mockBatch).getRecordListForShutdown();
    }

    @Test
    public void testApplyExceptionThenSuccess() throws InterruptedException {
        final RuntimeException testException = new RuntimeException("testApplyExceptionThenSuccess");
        commonWhensForTestApply(RETRY_COUNT);
        when(mockAmazonKinesisFirehose.putRecordBatch(any(PutRecordBatchRequest.class)))
                .thenThrow(testException).thenReturn(mockResult);

        firehoseProcessor.process(KEY, FULLY_POPULATED_SPAN);

        commonVerifiesForTestApply(1);
        commonVerifiesForTestApplyNotEmpty(2, 1);
        verify(mockSleeper).sleep(SLEEP_STEP);
        verify(mockLogger).warn(String.format(PUT_RECORD_BATCH_WARN_MSG, 0), testException);
        verify(mockCounters).countSuccessesAndFailures(mockRequest, null);
        verify(mockCounters).countSuccessesAndFailures(mockRequest, mockResult);
    }

    @Test
    public void testApplyExceptionAlways() throws InterruptedException {
        final RuntimeException testException = new RuntimeException("testApplyExceptionAlways");
        final int retryCount = 4;
        commonWhensForTestApply(retryCount);
        when(mockAmazonKinesisFirehose.putRecordBatch(any(PutRecordBatchRequest.class)))
                .thenThrow(testException);

        firehoseProcessor.process(KEY, FULLY_POPULATED_SPAN);

        commonVerifiesForTestApply(1);
        commonVerifiesForTestApplyNotEmpty(retryCount, 0);
        verify(mockSleeper).sleep(SLEEP_STEP);
        verify(mockSleeper).sleep(3 * SLEEP_STEP);
        verify(mockSleeper).sleep(7 * SLEEP_STEP);
        for(int i = 0 ; i < retryCount ; i++) {
            verify(mockLogger).warn(String.format(PUT_RECORD_BATCH_WARN_MSG, i), testException);
        }
        verify(mockCounters, times(retryCount)).countSuccessesAndFailures(mockRequest, null);
        verify(mockLogger).error(String.format(PUT_RECORD_BATCH_ERROR_MSG, 0, retryCount), testException);
    }

    @Test
    public void testApplyRetryCountExceeded() throws InterruptedException {
        commonWhensForTestApply(2);
        when(mockAmazonKinesisFirehose.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(mockResult);
        when(mockBatch.extractFailedRecords(
                any(PutRecordBatchRequest.class), any(PutRecordBatchResult.class), anyInt()))
                .thenReturn(mockRecordList);
        when(mockResult.getFailedPutCount()).thenReturn(1);
        when(mockCounters.countSuccessesAndFailures(any(PutRecordBatchRequest.class), any(PutRecordBatchResult.class)))
                .thenReturn(1);

        firehoseProcessor.process(KEY, FULLY_POPULATED_SPAN);

        commonVerifiesForTestApply(1);
        commonVerifiesForTestApplyNotEmpty(2, 2);
        verify(mockSleeper).sleep(SLEEP_STEP);
        verify(mockCounters, times(2)).countSuccessesAndFailures(mockRequest, mockResult);
        verify(mockBatch).extractFailedRecords(mockRequest, mockResult, 0);
        verify(mockBatch).extractFailedRecords(mockRequest, mockResult, 1);
        verify(mockLogger).error(String.format(PUT_RECORD_BATCH_ERROR_MSG, 1, 2), (Throwable) null);
    }

    @Test
    public void testApplyNullResult() throws InterruptedException {
        commonWhensForTestApply(RETRY_COUNT);
        when(mockAmazonKinesisFirehose.putRecordBatch(any(PutRecordBatchRequest.class)))
                .thenReturn(null).thenReturn(mockResult);
        when(mockBatch.extractFailedRecords(
                any(PutRecordBatchRequest.class), isNull(PutRecordBatchResult.class), anyInt()))
                .thenReturn(mockRecordList);

        firehoseProcessor.process(KEY, FULLY_POPULATED_SPAN);

        commonVerifiesForTestApply(1);
        commonVerifiesForTestApplyNotEmpty(2, 1);
        verify(mockSleeper).sleep(SLEEP_STEP);
        verify(mockCounters).countSuccessesAndFailures(mockRequest, null);
        verify(mockCounters).countSuccessesAndFailures(mockRequest, mockResult);
        verify(mockBatch).extractFailedRecords(mockRequest, null, 0);
    }

    private void commonWhensForTestApply(int retryCount) {
        when(mockBatch.getRecordList(any(Span.class))).thenReturn(mockRecordList);
        when(mockRecordList.isEmpty()).thenReturn(false);
        when(mockFactory.createPutRecordBatchRequest(anyString(), anyListOf(Record.class))).thenReturn(mockRequest);
        when(mockTimer.start()).thenReturn(mockStopwatch);
        when(mockFactory.createSleeper()).thenReturn(mockSleeper);
        when(mockFirehoseConfigurationProvider.retrycount()).thenReturn(Integer.toString(retryCount));
    }

    private void commonVerifiesForTestApply(int processTimes) {
        verify(mockCounters, times(processTimes)).incrementSpanCounter();
        verify(mockBatch, times(processTimes)).getRecordList(FULLY_POPULATED_SPAN);
        //noinspection ResultOfMethodCallIgnored
        verify(mockRecordList).isEmpty();
    }

    private void commonVerifiesForTestApplyNotEmpty(
            int wantedNumberOfInvocations, int failedPutCountTimes) throws InterruptedException {
        verify(mockFactory, times(wantedNumberOfInvocations)).createPutRecordBatchRequest(STREAM_NAME, mockRecordList);
        verify(mockTimer, times(wantedNumberOfInvocations)).start();
        verify(mockFactory, times(wantedNumberOfInvocations)).createSleeper();
        verify(mockSleeper).sleep(0);
        verify(mockAmazonKinesisFirehose, times(wantedNumberOfInvocations)).putRecordBatch(mockRequest);
        verify(mockStopwatch, times(wantedNumberOfInvocations)).stop();
        verify(mockFirehoseConfigurationProvider).retrycount();
        verify(mockResult, times(failedPutCountTimes)).getFailedPutCount();
    }

    @Test
    public void testInit() {
        wantedNumberOfInvocationsStreamName = 1;
        when(mockFactory.createShutdownHook(any(FirehoseProcessor.class))).thenReturn(mockThread);
        when(mockFactory.getRuntime()).thenReturn(mockRuntime);

        firehoseProcessor.init(mockProcessorContext);

        verify(mockFactory).createShutdownHook(firehoseProcessor);
        verify(mockFactory).getRuntime();
        verify(mockRuntime).addShutdownHook(mockThread);
    }

    @Test
    public void testPunctuate() {
        wantedNumberOfInvocationsStreamName = 1;
        firehoseProcessor.punctuate(TIMESTAMP);
    }

    @Test
    public void testSleeperSleep() throws InterruptedException {
        wantedNumberOfInvocationsStreamName = 1;
        sleeper.sleep(0);
    }

    @Test
    public void testFactoryCreatePutRecordBatchRequest() {
        wantedNumberOfInvocationsStreamName = 1;
        final List<Record> records = Collections.emptyList();
        final PutRecordBatchRequest request = factory.createPutRecordBatchRequest(STREAM_NAME, records);

        assertEquals(STREAM_NAME, request.getDeliveryStreamName());
        assertEquals(records, request.getRecords());
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
}
