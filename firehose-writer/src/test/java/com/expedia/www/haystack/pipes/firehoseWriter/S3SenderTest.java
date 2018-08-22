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

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class S3SenderTest {
    private static final String STREAM_NAME = RANDOM.nextLong() + "STREAM_NAME";
    private static final int SLEEP_MILLIS = RANDOM.nextInt();
    private static final int INITIAL_RETRY_SLEEP = RANDOM.nextInt(Byte.MAX_VALUE);
    private static final Integer MAX_RETRY_SLEEP = RANDOM.nextInt(Byte.MAX_VALUE);

    @Mock
    private Supplier<Batch> mockSupplierBatch;
    @Mock
    private Batch mockBatch;
    @Mock
    private FirehoseProcessor.Sleeper mockSleeper;
    @Mock
    private Factory mockFactory;
    @Mock
    private FirehoseConfigurationProvider mockFirehoseConfigurationProvider;
    @Mock
    private FirehoseCountersAndTimer mockFirehoseCountersAndTimer;
    @Mock
    private AmazonKinesisFirehoseAsync mockAmazonKinesisFirehoseAsync;
    @Mock
    private ArrayBlockingQueue<BlockingQueueEntry> mockArrayBlockingQueue;
    @Mock
    private RetryCalculator mockRetryCalculator;
    @Mock
    private Record mockRecord;
    @Mock
    private PutRecordBatchRequest mockPutRecordBatchRequest;
    @Mock
    private Stopwatch mockStopwatch;
    @Mock
    private Future<PutRecordBatchResult> mockPutRecordBatchResultFuture;
    @Mock
    private BlockingQueueEntry mockBlockingQueueEntry;
    @Mock
    private Thread mockThread;

    private S3Sender s3Sender;
    private List<Record> recordList;

    @Before
    public void setUp() {
        when(mockSupplierBatch.get()).thenReturn(mockBatch);
        when(mockFirehoseConfigurationProvider.streamname()).thenReturn(STREAM_NAME);
        s3Sender = new S3Sender(mockSupplierBatch, mockSleeper, mockFactory, mockFirehoseConfigurationProvider,
                mockFirehoseCountersAndTimer, mockAmazonKinesisFirehoseAsync, mockArrayBlockingQueue);
        recordList= new ArrayList<>(Collections.singleton(mockRecord));
    }

    @After
    public void tearDown() {
        verify(mockSupplierBatch).get();
        verify(mockFirehoseConfigurationProvider).streamname();

        verifyNoMoreInteractions(mockSupplierBatch);
        verifyNoMoreInteractions(mockBatch);
        verifyNoMoreInteractions(mockSleeper);
        verifyNoMoreInteractions(mockFactory);
        verifyNoMoreInteractions(mockFirehoseConfigurationProvider);
        verifyNoMoreInteractions(mockFirehoseCountersAndTimer);
        verifyNoMoreInteractions(mockAmazonKinesisFirehoseAsync);
        verifyNoMoreInteractions(mockArrayBlockingQueue);
        verifyNoMoreInteractions(mockRetryCalculator);
        verifyNoMoreInteractions(mockRecord);
        verifyNoMoreInteractions(mockPutRecordBatchRequest);
        verifyNoMoreInteractions(mockStopwatch);
        verifyNoMoreInteractions(mockPutRecordBatchResultFuture);
        verifyNoMoreInteractions(mockThread);
    }

    @Test
    public void testSendRecordsToS3EmptyList() throws InterruptedException {
        s3Sender.sendRecordsToS3(Collections.emptyList(), mockRetryCalculator);
    }

    private void whensForSendRecordsToS3NonEmptyList() {
        when(mockRetryCalculator.calculateSleepMillis()).thenReturn(SLEEP_MILLIS);
        when(mockFactory.createPutRecordBatchRequest(anyString(), anyListOf(Record.class)))
                .thenReturn(mockPutRecordBatchRequest);
        when(mockFirehoseCountersAndTimer.startTimer()).thenReturn(mockStopwatch);
        when(mockAmazonKinesisFirehoseAsync.putRecordBatchAsync(any()))
                .thenReturn(mockPutRecordBatchResultFuture);
        when(mockFactory.createBlockingQueueEntry(any(), any(), any(), any(), any()))
                .thenReturn(mockBlockingQueueEntry);
    }

    private void verifiesForSendRecordsToS3NonEmptyList() throws InterruptedException {
        verify(mockRetryCalculator).calculateSleepMillis();
        verify(mockSleeper).sleep(SLEEP_MILLIS);
        verify(mockFactory).createPutRecordBatchRequest(STREAM_NAME, recordList);
        verify(mockFirehoseCountersAndTimer).startTimer();
        verify(mockAmazonKinesisFirehoseAsync).putRecordBatchAsync(mockPutRecordBatchRequest);
        verify(mockFactory).createBlockingQueueEntry(mockStopwatch, mockRetryCalculator, mockPutRecordBatchRequest,
                mockBatch, mockPutRecordBatchResultFuture);
        verify(mockArrayBlockingQueue).put(mockBlockingQueueEntry);
    }

    @Test
    public void testSendRecordsToS3NonEmptyList() throws InterruptedException {
        whensForSendRecordsToS3NonEmptyList();

        s3Sender.sendRecordsToS3(recordList, mockRetryCalculator);

        verifiesForSendRecordsToS3NonEmptyList();
    }

    @Test
    public void testCloseHappyCase() throws InterruptedException {
        whensForClose();
        whensForSendRecordsToS3NonEmptyList();

        s3Sender.close();

        verifiesForClose();
        verifiesForSendRecordsToS3NonEmptyList();
    }

    @Test
    public void testCloseInterruptedException() throws InterruptedException {
        whensForClose();
        final InterruptedException interruptedException = new InterruptedException();
        doThrow(interruptedException).when(mockSleeper).sleep(anyLong());
        when(mockRetryCalculator.calculateSleepMillis()).thenReturn(SLEEP_MILLIS);
        when(mockFactory.currentThread()).thenReturn(mockThread);

        s3Sender.close();

        verifiesForClose();
        verify(mockRetryCalculator).calculateSleepMillis();
        verify(mockSleeper).sleep(SLEEP_MILLIS);
        verify(mockFactory).currentThread();
        verify(mockThread).interrupt();
    }

    private void whensForClose() {
        when(mockBatch.getRecordListForShutdown()).thenReturn(recordList);
        when(mockFirehoseConfigurationProvider.initialretrysleep()).thenReturn(INITIAL_RETRY_SLEEP);
        when(mockFirehoseConfigurationProvider.maxretrysleep()).thenReturn(MAX_RETRY_SLEEP);
        when(mockFactory.createRetryCalculator(anyInt(), anyInt())).thenReturn(mockRetryCalculator);
    }

    private void verifiesForClose() {
        verify(mockBatch).getRecordListForShutdown();
        verify(mockFirehoseConfigurationProvider).initialretrysleep();
        verify(mockFirehoseConfigurationProvider).maxretrysleep();
        verify(mockFactory).createRetryCalculator(INITIAL_RETRY_SLEEP, MAX_RETRY_SLEEP);
    }

}
