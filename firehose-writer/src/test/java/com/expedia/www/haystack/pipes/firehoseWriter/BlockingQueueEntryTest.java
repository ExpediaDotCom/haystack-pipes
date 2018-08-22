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
import java.util.concurrent.Future;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BlockingQueueEntryTest {
    private static final int UNBOUNDED_TRY_COUNT = RANDOM.nextInt(Byte.MAX_VALUE);
    @Mock
    private Stopwatch mockStopwatch;

    @Mock
    private RetryCalculator mockRetryCalculator;

    @Mock
    private PutRecordBatchRequest mockPutRecordBatchRequest;

    @Mock
    private Batch mockBatch;

    @Mock
    private Future<PutRecordBatchResult> mockFuture;

    @Mock
    private Record mockRecord;

    @Mock
    private PutRecordBatchResult mockRPutRecordBatchResult;

    private BlockingQueueEntry blockingQueueEntry;
    private List<Record> recordList;

    @Before
    public void setUp() {
        blockingQueueEntry = new BlockingQueueEntry(
                mockStopwatch, mockRetryCalculator, mockPutRecordBatchRequest, mockBatch, mockFuture);
        recordList = Collections.singletonList(mockRecord);
    }

    @SuppressWarnings("Duplicates")
    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockStopwatch);
        verifyNoMoreInteractions(mockRetryCalculator);
        verifyNoMoreInteractions(mockPutRecordBatchRequest);
        verifyNoMoreInteractions(mockBatch);
        verifyNoMoreInteractions(mockFuture);
        verifyNoMoreInteractions(mockRecord);
        verifyNoMoreInteractions(mockRPutRecordBatchResult);
    }

    @Test
    public void testConstructor() {
        assertSame(mockStopwatch, blockingQueueEntry.getStopwatch());
        assertSame(mockRetryCalculator, blockingQueueEntry.getRetryCalculator());
        assertSame(mockPutRecordBatchRequest, blockingQueueEntry.getPutRecordBatchRequest());
        assertSame(mockBatch, blockingQueueEntry.getBatch());
        assertSame(mockFuture, blockingQueueEntry.getFuturePutRecordBatchResult());
    }

    @Test
    public void testExtractFailedRecordsNullResult() {
        when(mockPutRecordBatchRequest.getRecords()).thenReturn(recordList);

        assertEquals(recordList, blockingQueueEntry.extractFailedRecords(null));

        verify(mockPutRecordBatchRequest).getRecords();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testExtractFailedRecordsNonNullResult() {
        when(mockRetryCalculator.getUnboundedTryCount()).thenReturn(UNBOUNDED_TRY_COUNT);
        when(mockBatch.extractFailedRecords(
                any(PutRecordBatchRequest.class), any(PutRecordBatchResult.class), anyInt()))
                .thenReturn(recordList);

        assertEquals(recordList, blockingQueueEntry.extractFailedRecords(mockRPutRecordBatchResult));

        verify(mockRetryCalculator).getUnboundedTryCount();
        verify(mockBatch).extractFailedRecords(
                mockPutRecordBatchRequest, mockRPutRecordBatchResult, UNBOUNDED_TRY_COUNT);
    }
}
