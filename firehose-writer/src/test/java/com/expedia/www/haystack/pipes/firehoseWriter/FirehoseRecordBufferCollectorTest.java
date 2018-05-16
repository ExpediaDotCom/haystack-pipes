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

import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.List;

import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.www.haystack.pipes.firehoseWriter.FirehoseRecordBufferCollector.Factory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseRecordBufferCollector.LAST_BATCH_TIME_DIFF_ALLOWED_MILLIS;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseRecordBufferCollector.MAX_BYTES_IN_BATCH;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseRecordBufferCollector.MAX_RECORDS_IN_BATCH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseRecordBufferCollectorTest {
    private static final ByteBuffer EMPTY_DATA = ByteBuffer.allocate(0);
    private static final ByteBuffer ONE_BYTE_DATA = ByteBuffer.allocate(1);
    private static final ByteBuffer FULL_DATA = ByteBuffer.allocate(MAX_BYTES_IN_BATCH);
    private static final long BATCH_LAST_CREATED_AT = RANDOM.nextInt(Integer.MAX_VALUE);
    private static final long TIMEOUT_INSTANT_MILLIS = BATCH_LAST_CREATED_AT + LAST_BATCH_TIME_DIFF_ALLOWED_MILLIS;

    @Mock
    private Record mockRecord;

    @Mock
    private Factory mockFactory;

    @Mock
    private Clock mockClock;

    private int timesClockMillis = 1;
    private FirehoseRecordBufferCollector firehoseCollector;
    private Factory factory;

    @Before
    public void setUp() {
        when(mockClock.millis()).thenReturn(BATCH_LAST_CREATED_AT);
        firehoseCollector = new FirehoseRecordBufferCollector(mockFactory, mockClock);
        factory = new Factory();
    }

    @After
    public void tearDown() {
        verify(mockClock, times(timesClockMillis)).millis();
        verifyNoMoreInteractions(mockRecord, mockClock);
    }

    @Test
    public void testShouldCreateNewBatchEmptyList() {
        when(mockRecord.getData()).thenReturn(FULL_DATA);

        assertFalse(firehoseCollector.shouldCreateNewBatch(mockRecord));

        verify(mockRecord).getData();
    }

    @Test
    public void testShouldCreateNewBatchTooLarge() {
        timesClockMillis = 2;
        when(mockRecord.getData()).thenReturn(ONE_BYTE_DATA);

        firehoseCollector.addRecordAndReturnBatch(FULL_DATA.array());
        firehoseCollector.addRecordAndReturnBatch(ONE_BYTE_DATA.array());

        assertFalse(firehoseCollector.shouldCreateNewBatch(mockRecord));
        verify(mockRecord, times(1)).getData();
    }

    @Test
    public void testShouldCreateNewBatchRoomForJustOneMoreRecord() {
        testShouldCreateNewBatch(MAX_RECORDS_IN_BATCH - 2, false);
    }

    @Test
    public void testShouldCreateNewBatchNoMoreRoom() {
        timesClockMillis = 2;
        testShouldCreateNewBatch(MAX_RECORDS_IN_BATCH, true);

        final List<Record> batch = firehoseCollector.addRecordAndReturnBatch(FULL_DATA.array());

        assertEquals(MAX_RECORDS_IN_BATCH, batch.size());
    }

    private void testShouldCreateNewBatch(int recordCount, boolean expected) {
        when(mockRecord.getData()).thenReturn(EMPTY_DATA);
        for (int i = 0; i < recordCount; i++) {
            final List<Record> batch = firehoseCollector.addRecordAndReturnBatch(ONE_BYTE_DATA.array());
            final boolean isEmpty = batch.isEmpty();
            assertTrue(isEmpty);
        }

        assertEquals(expected, firehoseCollector.shouldCreateNewBatch(mockRecord));

        verify(mockRecord, times(1)).getData();
    }

    @Test
    public void testReturnIncompleteBatch() {
        timesClockMillis = 3;
        testShouldCreateNewBatch(MAX_RECORDS_IN_BATCH / 2, false);

        final List<Record> batch = firehoseCollector.returnIncompleteBatch();

        assertEquals(MAX_RECORDS_IN_BATCH / 2, batch.size());
        assertEquals(0, firehoseCollector.returnIncompleteBatch().size());
    }

    @Test
    public void testBatchCreationTimedOutOneMillisecondEarly() {
        testBatchCreationTimedOut(false, TIMEOUT_INSTANT_MILLIS - 1);
    }

    @Test
    public void testBatchCreationTimedOutTimesAreEqual() {
        testBatchCreationTimedOut(false, TIMEOUT_INSTANT_MILLIS);
    }

    @Test
    public void testBatchCreationTimedOutOneMillisecondLate() {
        testBatchCreationTimedOut(true, TIMEOUT_INSTANT_MILLIS + 1);
    }

    private void testBatchCreationTimedOut(boolean expected, long currentTimeMillis) {
        when(mockRecord.getData()).thenReturn(EMPTY_DATA);
        when(mockFactory.currentTimeMillis()).thenReturn(currentTimeMillis);

        assertEquals(expected, firehoseCollector.shouldCreateNewBatch(mockRecord));

        verify(mockRecord).getData();
        verify(mockFactory).currentTimeMillis();
    }

    @Test
    public void testFactoryCurrentTimeMillis() {
        assertTrue(System.currentTimeMillis() <= factory.currentTimeMillis());
    }

}
