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
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseConsumer.EXECUTION_EXCEPTION_ERROR_MSG;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseConsumer.PUT_RECORD_BATCH_ERROR_MSG;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseConsumer.STARTUP_MSG;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class FirehoseConsumerTest {
    private static final int UNBOUNDED_TRY_COUNT = RANDOM.nextInt(Byte.MAX_VALUE);
    private static final InterruptedException INTERRUPTED_EXCEPTION = new InterruptedException("Test");
    private static final ExecutionException EXECUTION_EXCEPTION = new ExecutionException(INTERRUPTED_EXCEPTION);

    @Mock
    private S3Sender mockS3Sender;
    @Mock
    private Logger mockLogger;
    @Mock
    private FirehoseCountersAndTimer mockFirehoseCountersAndTimer;
    @Mock
    private ArrayBlockingQueue<BlockingQueueEntry> mockArrayBlockingQueue;
    @Mock
    private Future<PutRecordBatchResult> mockFuturePutRecordBatchResult;
    @Mock
    private PutRecordBatchResult mockPutRecordBatchResult;
    @Mock
    private BlockingQueueEntry mockBlockingQueueEntry;
    @Mock
    private Stopwatch mockStopwatch;
    @Mock
    private PutRecordBatchRequest mockPutRecordBatchRequest;
    @Mock
    private RetryCalculator mockRetryCalculator;
    @Mock
    private Record mockRecord;

    private FirehoseConsumer firehoseConsumer;

    @Before
    public void setUp() {
        firehoseConsumer = new FirehoseConsumer(
                mockS3Sender, mockLogger, mockFirehoseCountersAndTimer, mockArrayBlockingQueue);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockS3Sender);
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockFirehoseCountersAndTimer);
        verifyNoMoreInteractions(mockArrayBlockingQueue);
        verifyNoMoreInteractions(mockFuturePutRecordBatchResult);
        verifyNoMoreInteractions(mockPutRecordBatchResult);
        verifyNoMoreInteractions(mockBlockingQueueEntry);
        verifyNoMoreInteractions(mockStopwatch);
        verifyNoMoreInteractions(mockPutRecordBatchRequest);
        verifyNoMoreInteractions(mockRetryCalculator);
        verifyNoMoreInteractions(mockRecord);
    }

    @Test
    public void testRunHappyCase() throws InterruptedException, ExecutionException {
        whensForTestRunTake();
        whensForTestRunGetPutRecordBatchRequest();
        whensForTestRunGet();
        whensForTestExtractFailedRecords(Collections.emptyList());

        firehoseConsumer.run();

        verifiesForTestRunTake(2);
        verifiesForTestRunGetFuturePutRecordBatchResult(1);
        verifiesForTestRunGetPutRecordBatchRequest(1);
        verifiesForTestExtractFailedRecords(1);
    }

    @Test
    public void testRunSuccessfulRetry() throws InterruptedException, ExecutionException {
        testRunSuccessfulRetry(Collections.singletonList(mockRecord));
    }

    @Test
    public void testRunSuccessfulRetrySendRecordsToS3InterruptedException()
            throws InterruptedException, ExecutionException {
        final List<Record> failedRecords = Collections.singletonList(mockRecord);
        whensForTestRunTake(mockBlockingQueueEntry);
        whensForTestRunGetPutRecordBatchRequest(mockFuturePutRecordBatchResult);
        whensForTestRunGet(mockPutRecordBatchResult);
        whensForTestExtractFailedRecords(failedRecords);
        doThrow(INTERRUPTED_EXCEPTION).when(mockS3Sender)
                .sendRecordsToS3(anyListOf(Record.class), any(RetryCalculator.class));

        firehoseConsumer.run();

        verifiesForTestRunTake(1);
        verifiesForTestRunGetFuturePutRecordBatchResult(1);
        verifiesForTestRunGetPutRecordBatchRequest(1);
        verifiesForTestExtractFailedRecords(1);
        verifiesForTestRunSendRecordsToS3(failedRecords);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testRunSuccessfulRetryTryCountBeyondLimit() throws InterruptedException, ExecutionException {
        when(mockRetryCalculator.getUnboundedTryCount()).thenReturn(UNBOUNDED_TRY_COUNT);
        when(mockRetryCalculator.isTryCountBeyondLimit()).thenReturn(true);
        testRunSuccessfulRetry(Collections.singletonList(mockRecord));
        verify(mockRetryCalculator).getUnboundedTryCount();
        verify(mockLogger).error(PUT_RECORD_BATCH_ERROR_MSG, 1, UNBOUNDED_TRY_COUNT);
    }

    private void testRunSuccessfulRetry(List<Record> failedRecords) throws InterruptedException, ExecutionException {
        whensForTestRunTake(mockBlockingQueueEntry);
        whensForTestRunGetPutRecordBatchRequest(mockFuturePutRecordBatchResult);
        whensForTestRunGet(mockPutRecordBatchResult);
        whensForTestExtractFailedRecords(failedRecords);

        firehoseConsumer.run();

        verifiesForTestRunTake(3);
        verifiesForTestRunGetFuturePutRecordBatchResult(2);
        verifiesForTestRunGetPutRecordBatchRequest(2);
        verifiesForTestExtractFailedRecords(2);
        verifiesForTestRunSendRecordsToS3(failedRecords);
    }

    @Test
    public void testRunExecutionException() throws InterruptedException, ExecutionException {
        final List<Record> failedRecords = Collections.singletonList(mockRecord);
        whensForTestRunTake(mockBlockingQueueEntry);
        whensForTestRunGetPutRecordBatchRequest(mockFuturePutRecordBatchResult);
        whensForTestRunGet();
        whensForTestExtractFailedRecords(failedRecords);

        firehoseConsumer.run();

        verifiesForTestRunTake(3);
        verifiesForTestRunGetFuturePutRecordBatchResult(2);
        verifiesForTestRunGetPutRecordBatchRequest(2);
        verifiesForTestExtractFailedRecords(1);
        verifiesForTestRunSendRecordsToS3(failedRecords);
        verify(mockLogger).error(EXECUTION_EXCEPTION_ERROR_MSG, EXECUTION_EXCEPTION);
        verify(mockFirehoseCountersAndTimer).incrementExceptionCounter();
        verify(mockFirehoseCountersAndTimer).countSuccessesAndFailures(mockPutRecordBatchRequest, null);
        verify(mockBlockingQueueEntry).extractFailedRecords(null);
    }

    private void whensForTestRunTake(BlockingQueueEntry... blockingQueueEntries) throws InterruptedException {
        when(mockArrayBlockingQueue.take())
                .thenReturn(mockBlockingQueueEntry, blockingQueueEntries)
                .thenThrow(INTERRUPTED_EXCEPTION);
    }

    private void whensForTestRunGetPutRecordBatchRequest(Future<PutRecordBatchResult>... futurePutRecordBatchRequests) {
        when(mockBlockingQueueEntry.getFuturePutRecordBatchResult())
                .thenReturn(mockFuturePutRecordBatchResult, futurePutRecordBatchRequests);
    }

    private void whensForTestRunGet(PutRecordBatchResult... putRecordBatchResults)
            throws ExecutionException, InterruptedException {
        when(mockFuturePutRecordBatchResult.get())
                .thenReturn(mockPutRecordBatchResult, putRecordBatchResults)
                .thenThrow(EXECUTION_EXCEPTION);
        when(mockBlockingQueueEntry.getStopwatch()).thenReturn(mockStopwatch);
        when(mockBlockingQueueEntry.getPutRecordBatchRequest()).thenReturn(mockPutRecordBatchRequest);
        when(mockBlockingQueueEntry.getRetryCalculator()).thenReturn(mockRetryCalculator);
    }

    private void whensForTestExtractFailedRecords(List<Record> failedRecords) {
        when(mockBlockingQueueEntry.extractFailedRecords(any(PutRecordBatchResult.class)))
                .thenReturn(failedRecords)
                .thenReturn(Collections.emptyList());
        when(mockRetryCalculator.getUnboundedTryCount()).thenReturn(UNBOUNDED_TRY_COUNT);
    }

    private void verifiesForTestRunTake(int wantedNumberOfInvocations) throws InterruptedException {
        verify(mockLogger).info(STARTUP_MSG);
        verify(mockArrayBlockingQueue, times(wantedNumberOfInvocations)).take();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void verifiesForTestRunGetFuturePutRecordBatchResult(int wantedNumberOfInvocations)
            throws InterruptedException, ExecutionException {
        verify(mockBlockingQueueEntry, times(wantedNumberOfInvocations)).getFuturePutRecordBatchResult();
        verify(mockFuturePutRecordBatchResult, times(wantedNumberOfInvocations)).get();
        verify(mockBlockingQueueEntry, times(wantedNumberOfInvocations)).getStopwatch();
        verify(mockStopwatch, times(wantedNumberOfInvocations)).stop();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void verifiesForTestRunGetPutRecordBatchRequest(int wantedNumberOfInvocations) {
        verify(mockBlockingQueueEntry, times(wantedNumberOfInvocations)).getPutRecordBatchRequest();
    }

    private void verifiesForTestExtractFailedRecords(int wantedNumberOfInvocations) {
        verify(mockFirehoseCountersAndTimer, times(wantedNumberOfInvocations)).countSuccessesAndFailures(
                mockPutRecordBatchRequest, mockPutRecordBatchResult);
        verify(mockBlockingQueueEntry, times(wantedNumberOfInvocations)).extractFailedRecords(mockPutRecordBatchResult);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void verifiesForTestRunSendRecordsToS3(List<Record> failedRecords) throws InterruptedException {
        verify(mockBlockingQueueEntry, times(failedRecords.size())).getRetryCalculator();
        verify(mockRetryCalculator, times(failedRecords.size())).isTryCountBeyondLimit();
        verify(mockS3Sender).sendRecordsToS3(failedRecords, mockRetryCalculator);
    }
}
