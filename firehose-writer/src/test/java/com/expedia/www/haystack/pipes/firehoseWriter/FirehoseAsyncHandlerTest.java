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

import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.netflix.servo.monitor.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseAsyncHandlerTest {
    private static final int RETRY_COUNT = RANDOM.nextInt(Byte.MAX_VALUE);
    private static final int SLEEP_MILLIS = RANDOM.nextInt(Byte.MAX_VALUE);

    @Mock
    private Exception mockException;
    @Mock
    private FailedRecordExtractor mockFailedRecordExtractor;
    @Mock
    private FirehoseTimersAndCounters mockFirehoseCountersAndTimer;
    @Mock
    private List<Record> mockRecordList;
    @Mock
    private PutRecordBatchRequest mockPutRecordBatchRequest;
    @Mock
    private PutRecordBatchResult mockPutRecordBatchResult;
    @Mock
    private Record mockRecord;
    @Mock
    private RetryCalculator mockRetryCalculator;
    @Mock
    private S3Sender mockS3Sender;
    @Mock
    private Callback mockCallback;
    @Mock
    private Sleeper mockSleeper;
    @Mock
    private Stopwatch mockStopwatch;

    private FirehoseAsyncHandler firehoseAsyncHandler;
    private List<Record> recordList;

    @Before
    public void setUp() {
        firehoseAsyncHandler = new FirehoseAsyncHandler(mockS3Sender,
                mockStopwatch,
                mockPutRecordBatchRequest,
                SLEEP_MILLIS,
                RETRY_COUNT,
                mockRecordList,
                mockRetryCalculator,
                mockSleeper,
                mockFirehoseCountersAndTimer,
                mockFailedRecordExtractor,
                mockCallback);
        recordList = Collections.singletonList(mockRecord);
    }

    @SuppressWarnings("Duplicates")
    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockException);
        verifyNoMoreInteractions(mockFailedRecordExtractor);
        verifyNoMoreInteractions(mockFirehoseCountersAndTimer);
        verifyNoMoreInteractions(mockRecordList);
        verifyNoMoreInteractions(mockPutRecordBatchRequest);
        verifyNoMoreInteractions(mockPutRecordBatchResult);
        verifyNoMoreInteractions(mockRecord);
        verifyNoMoreInteractions(mockRetryCalculator);
        verifyNoMoreInteractions(mockS3Sender);
        verifyNoMoreInteractions(mockCallback);
        verifyNoMoreInteractions(mockSleeper);
        verifyNoMoreInteractions(mockStopwatch);
    }

    @Test
    public void testOnError() {
        firehoseAsyncHandler.onError(mockException);

        verify(mockS3Sender).onFirehoseCallback(mockStopwatch, mockPutRecordBatchRequest, null, SLEEP_MILLIS,
                RETRY_COUNT, mockException, mockFirehoseCountersAndTimer);
        verify(mockS3Sender).sendRecordsToS3(
                mockRecordList, mockRetryCalculator, mockSleeper, RETRY_COUNT + 1, mockCallback);
    }

    @Test
    public void testOnSuccessNoFailures() {
        testOnSuccess(0);

        verify(mockCallback).onComplete();
    }

    @Test
    public void testOnSuccessFailures() {
        when(mockFailedRecordExtractor.extractFailedRecords(any(), any(), anyInt())).thenReturn(recordList);

        testOnSuccess(1);

        verify(mockFailedRecordExtractor).extractFailedRecords(
                mockPutRecordBatchRequest, mockPutRecordBatchResult, RETRY_COUNT);
        verify(mockS3Sender).sendRecordsToS3(
                recordList, mockRetryCalculator, mockSleeper, RETRY_COUNT + 1, mockCallback);
    }

    private void testOnSuccess(int failureCount) {
        when(mockPutRecordBatchResult.getFailedPutCount()).thenReturn(failureCount);
        when(mockS3Sender.areThereRecordsThatFirehoseHasNotProcessed(anyInt())).thenReturn(failureCount > 0);

        firehoseAsyncHandler.onSuccess(mockPutRecordBatchRequest, mockPutRecordBatchResult);

        verify(mockS3Sender).onFirehoseCallback(mockStopwatch, mockPutRecordBatchRequest, mockPutRecordBatchResult,
                SLEEP_MILLIS, RETRY_COUNT, null, mockFirehoseCountersAndTimer);
        verify(mockPutRecordBatchResult).getFailedPutCount();
        verify(mockS3Sender).areThereRecordsThatFirehoseHasNotProcessed(failureCount);
    }
}
