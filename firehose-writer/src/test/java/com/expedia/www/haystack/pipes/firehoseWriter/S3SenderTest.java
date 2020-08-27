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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.www.haystack.pipes.commons.kafka.config.FirehoseConfig;
import com.expedia.www.haystack.pipes.firehoseWriter.S3Sender.Factory;
import com.netflix.servo.monitor.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.PUT_RECORD_BATCH_ERROR_MSG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class S3SenderTest {
    private static final int RETRY_COUNT = RANDOM.nextInt(Byte.MAX_VALUE);
    private static final int SLEEP_MILLIS = 2_718;
    private static final int MAX_RETRY_SLEEP = 10_000;
    private static final String STREAM_NAME = RANDOM.nextLong() + "STREAM_NAME";
    private static final InterruptedException INTERRUPTED_EXCEPTION = new InterruptedException();

    @Mock
    private AmazonKinesisFirehoseAsync mockAmazonKinesisFirehoseAsync;
    @Mock
    private Exception mockException;
    @Mock
    private Factory mockFactory;
    @Mock
    private FailedRecordExtractor mockFailedRecordExtractor;
    @Mock
    private FirehoseAsyncHandler mockFirehoseAsyncHandler;
    @Mock
    private FirehoseConfig mockFirehoseConfig;
    @Mock
    private FirehoseTimersAndCounters mockFirehoseCountersAndTimer;
    @Mock
    private Logger mockLogger;
    @Mock
    private PutRecordBatchRequest mockPutRecordBatchRequest;
    @Mock
    private PutRecordBatchResult mockPutRecordBatchResult;
    @Mock
    private Record mockRecord;
    @Mock
    private RetryCalculator mockRetryCalculator;
    @Mock
    private Callback mockCallback;
    @Mock
    private Sleeper mockSleeper;
    @Mock
    private Stopwatch mockStopwatch;
    @Mock
    private Thread mockThread;
    @Mock
    private UnexpectedExceptionLogger mockUnexpectedExceptionLogger;

    private S3Sender s3Sender;
    private List<Record> recordList;
    private Factory factory;

    @Before
    public void setUp() {
        s3Sender = new S3Sender(mockFirehoseConfig, mockFactory, mockFirehoseCountersAndTimer,
                mockAmazonKinesisFirehoseAsync, mockLogger, mockFailedRecordExtractor, mockUnexpectedExceptionLogger);
        factory = new Factory();
        recordList = Collections.singletonList(mockRecord);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockAmazonKinesisFirehoseAsync);
        verifyNoMoreInteractions(mockException);
        verifyNoMoreInteractions(mockFactory);
        verifyNoMoreInteractions(mockFailedRecordExtractor);
        verifyNoMoreInteractions(mockFirehoseAsyncHandler);
        verifyNoMoreInteractions(mockFirehoseConfig);
        verifyNoMoreInteractions(mockFirehoseCountersAndTimer);
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockPutRecordBatchRequest);
        verifyNoMoreInteractions(mockPutRecordBatchResult);
        verifyNoMoreInteractions(mockRecord);
        verifyNoMoreInteractions(mockRetryCalculator);
        verifyNoMoreInteractions(mockCallback);
        verifyNoMoreInteractions(mockSleeper);
        verifyNoMoreInteractions(mockStopwatch);
        verifyNoMoreInteractions(mockThread);
        verifyNoMoreInteractions(mockUnexpectedExceptionLogger);
    }

    @Test
    public void testSendRecordsToS3HappyCase() throws InterruptedException {
        whenForSendRecordsToS3();

        s3Sender.sendRecordsToS3(recordList, mockRetryCalculator, mockSleeper, RETRY_COUNT, mockCallback);

        verifiesForSendRecordsToS3();
    }

    @Test
    public void testSendRecordsToS3Interrupted() throws InterruptedException {
        doThrow(INTERRUPTED_EXCEPTION).when(mockSleeper).sleep(anyLong());
        when(mockFactory.currentThread()).thenReturn(mockThread);

        testSendRecordsToS3HappyCase();

        verify(mockFactory).currentThread();
        verify(mockThread).interrupt();
    }

    private void whenForSendRecordsToS3() {
        when(mockFirehoseConfig.getStreamName()).thenReturn(STREAM_NAME);
        when(mockFactory.createPutRecordBatchRequest(anyString(), any())).thenReturn(mockPutRecordBatchRequest);
        when(mockFirehoseCountersAndTimer.startTimer()).thenReturn(mockStopwatch);
        when(mockRetryCalculator.calculateSleepMillis()).thenReturn(SLEEP_MILLIS);
        when(mockFactory.createSleeper()).thenReturn(mockSleeper);
        when(mockFactory.createFirehoseAsyncHandler(any(), any(), any(), anyInt(), anyInt(), anyListOf(Record.class),
                any(), any(), any(), any(), any())).thenReturn(mockFirehoseAsyncHandler);
    }

    private void verifiesForSendRecordsToS3() throws InterruptedException {
        verify(mockFirehoseConfig).getStreamName();
        verify(mockFactory).createPutRecordBatchRequest(STREAM_NAME, recordList);
        verify(mockFirehoseCountersAndTimer).startTimer();
        verify(mockRetryCalculator).calculateSleepMillis();
        verify(mockFactory).createSleeper();
        verify(mockSleeper).sleep(SLEEP_MILLIS);
        verify(mockFactory).createFirehoseAsyncHandler(s3Sender, mockStopwatch, mockPutRecordBatchRequest, SLEEP_MILLIS,
                RETRY_COUNT, recordList, mockRetryCalculator, mockSleeper, mockFirehoseCountersAndTimer,
                mockFailedRecordExtractor, mockCallback);
        verify(mockAmazonKinesisFirehoseAsync).putRecordBatchAsync(mockPutRecordBatchRequest, mockFirehoseAsyncHandler);
    }

    @Test
    public void testOnFirehoseCallbackNoLogging() {
        testOnFirehoseCallback(0, SLEEP_MILLIS, null);
    }

    @Test
    public void testOnFirehoseCallbackWithLogging() {
        testOnFirehoseCallback(1, MAX_RETRY_SLEEP, mockException);
    }

    @Test
    public void testOnFirehoseCallbackWithLoggingSdkClientException() {
        testOnFirehoseCallback(1, MAX_RETRY_SLEEP, new SdkClientException("Test"));
    }

    @Test
    public void testOnFirehoseCallbackWithCountingSdkClientException() {
        testOnFirehoseCallback(1, MAX_RETRY_SLEEP, new SdkClientException(new SocketTimeoutException()));
    }

    private void testOnFirehoseCallback(int failureCount, int maxRetrySleep, Exception exception) {
        when(mockFirehoseCountersAndTimer.countSuccessesAndFailures(any(), any())).thenReturn(failureCount);
        when(mockFirehoseConfig.getMaxRetrySleep()).thenReturn(MAX_RETRY_SLEEP);

        s3Sender.onFirehoseCallback(mockStopwatch, mockPutRecordBatchRequest, mockPutRecordBatchResult, maxRetrySleep,
                RETRY_COUNT, exception, mockFirehoseCountersAndTimer);

        verify(mockStopwatch).stop();
        verify(mockFirehoseCountersAndTimer)
                .countSuccessesAndFailures(mockPutRecordBatchRequest, mockPutRecordBatchResult);
        verify(mockFirehoseConfig).getMaxRetrySleep();

        verify(mockLogger, times(failureCount)).error(
                String.format(PUT_RECORD_BATCH_ERROR_MSG, failureCount, RETRY_COUNT));
        if (exception instanceof SdkClientException && exception.getCause() instanceof SocketTimeoutException) {
            verify(mockFirehoseCountersAndTimer).incrementSocketTimeoutCounter();
        } else {
            verify(mockUnexpectedExceptionLogger, times(failureCount)).logError(exception);
        }
    }

    @Test
    public void testFactoryCreateFirehoseAsyncHandler() {
        final FirehoseAsyncHandler firehoseAsyncHandler = factory.createFirehoseAsyncHandler(s3Sender, mockStopwatch,
                mockPutRecordBatchRequest, SLEEP_MILLIS, RETRY_COUNT, recordList, mockRetryCalculator, mockSleeper,
                mockFirehoseCountersAndTimer, mockFailedRecordExtractor, mockCallback);

        assertSame(s3Sender, firehoseAsyncHandler.s3Sender);
        assertSame(mockStopwatch, firehoseAsyncHandler.stopwatch);
        assertSame(mockPutRecordBatchRequest, firehoseAsyncHandler.putRecordBatchRequest);
        assertEquals(SLEEP_MILLIS, firehoseAsyncHandler.sleepMillis);
        assertEquals(RETRY_COUNT, firehoseAsyncHandler.retryCount);
        assertSame(recordList, firehoseAsyncHandler.recordList);
        assertSame(mockRetryCalculator, firehoseAsyncHandler.retryCalculator);
        assertSame(mockSleeper, firehoseAsyncHandler.sleeper);
        assertSame(mockCallback, firehoseAsyncHandler.callback);
        assertSame(mockFirehoseCountersAndTimer, firehoseAsyncHandler.firehoseTimersAndCounters);
        assertSame(mockFailedRecordExtractor, firehoseAsyncHandler.failedRecordExtractor);
    }

    @Test
    public void testFactoryPutRecordBatchRequest() {
        final PutRecordBatchRequest putRecordBatchRequest =
                factory.createPutRecordBatchRequest(STREAM_NAME, recordList);
        assertEquals(STREAM_NAME, putRecordBatchRequest.getDeliveryStreamName());
        assertEquals(recordList, putRecordBatchRequest.getRecords());
    }

    @Test
    public void testShouldLogErrorMessage() {
        final Object[][] dataForCall = {
            {0, 0, 0, false}, // hasSleepMillisReachedItsLimit()  && !areThereRecordsThatFirehoseHasNotProcessed()
            {0, 1, 0, false}, // !hasSleepMillisReachedItsLimit() && !areThereRecordsThatFirehoseHasNotProcessed()
            {1, 0, 0, true},  //  hasSleepMillisReachedItsLimit() &&  areThereRecordsThatFirehoseHasNotProcessed()
            {1, 0, 1, false}, // !hasSleepMillisReachedItsLimit() &&  areThereRecordsThatFirehoseHasNotProcessed()
        };
        for (Object[] datumForCall : dataForCall) {
            final int failureCount = (Integer) datumForCall[0];
            final int maxRetrySleep = (Integer) datumForCall[1];
            final int sleepMillis = (Integer) datumForCall[2];
            final boolean expected = (Boolean) datumForCall[3];
            final boolean actual = s3Sender.shouldLogErrorMessage(failureCount, maxRetrySleep, sleepMillis);
            assertEquals(expected, actual);
        }

    }
}
