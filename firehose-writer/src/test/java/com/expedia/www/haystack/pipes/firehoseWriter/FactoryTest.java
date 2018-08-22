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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class FactoryTest {
    private static final String STREAM_NAME = RANDOM.nextLong() + "STREAM_NAME";
    private static final int INITIAL_RETRY_SLEEP = RANDOM.nextInt();
    private static final int MAX_RETRY_SLEEP = RANDOM.nextInt();

    @Mock
    private FirehoseProcessor mockFirehoseProcessor;
    @Mock
    private Stopwatch mockStopwatch;
    @Mock
    private RetryCalculator mockRetryCalculator;
    @Mock
    private PutRecordBatchRequest mockPutRecordBatchRequest;
    @Mock
    private Batch mockBatch;
    @Mock
    private Future<PutRecordBatchResult> mockPutRecordBatchResultFuture;

    private Factory factory;

    @Before
    public void setUp() {
        factory = new Factory();
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockFirehoseProcessor);
        verifyNoMoreInteractions(mockStopwatch);
        verifyNoMoreInteractions(mockRetryCalculator);
        verifyNoMoreInteractions(mockPutRecordBatchRequest);
        verifyNoMoreInteractions(mockBatch);
        verifyNoMoreInteractions(mockPutRecordBatchResultFuture);
    }

    @Test
    public void testFactoryCreatePutRecordBatchRequest() {
        final List<Record> records = Collections.emptyList();
        final PutRecordBatchRequest request = factory.createPutRecordBatchRequest(STREAM_NAME, records);

        assertEquals(STREAM_NAME, request.getDeliveryStreamName());
        assertEquals(records, request.getRecords());
    }

    @Test
    public void testFactoryGetRuntime() {
        assertSame(Runtime.getRuntime(), factory.getRuntime());
    }

    @Test
    public void testFactoryCreateShutdownHookAndShutdownHookClass() {
        factory.createShutdownHook(mockFirehoseProcessor).run();

        verify(mockFirehoseProcessor).close();
    }

    @Test
    public void testCurrentThread() {
        assertSame(Thread.currentThread(), factory.currentThread());
    }

    @Test
    public void testCreateRetryCalculator() {
        final RetryCalculator retryCalculator = factory.createRetryCalculator(INITIAL_RETRY_SLEEP, MAX_RETRY_SLEEP);

        assertEquals(INITIAL_RETRY_SLEEP, retryCalculator.initialRetrySleep);
        assertEquals(MAX_RETRY_SLEEP, retryCalculator.maxRetrySleep);
    }

    @Test
    public void testCreateBlockingQueueEntry() {
        final BlockingQueueEntry blockingQueueEntry = factory.createBlockingQueueEntry(mockStopwatch,
                mockRetryCalculator, mockPutRecordBatchRequest, mockBatch, mockPutRecordBatchResultFuture);

        assertSame(mockStopwatch, blockingQueueEntry.getStopwatch());
        assertSame(mockRetryCalculator, blockingQueueEntry.getRetryCalculator());
        assertSame(mockPutRecordBatchRequest, blockingQueueEntry.getPutRecordBatchRequest());
        assertSame(mockBatch, blockingQueueEntry.getBatch());
        assertSame(mockPutRecordBatchResultFuture, blockingQueueEntry.getFuturePutRecordBatchResult());
    }
}
