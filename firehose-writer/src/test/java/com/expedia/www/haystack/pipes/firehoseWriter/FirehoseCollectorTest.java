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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.List;

import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseCollector.MAX_BYTES_IN_BATCH;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseCollector.MAX_RECORDS_IN_BATCH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseCollectorTest {
    private static final ByteBuffer EMPTY_DATA = ByteBuffer.allocate(0);
    private static final ByteBuffer ONE_BYTE_DATA = ByteBuffer.allocate(1);
    private static final ByteBuffer FULL_DATA = ByteBuffer.allocate(MAX_BYTES_IN_BATCH);

    @Mock
    private Record mockRecord;

    private FirehoseCollector firehoseCollector;

    @Before
    public void setUp() {
        firehoseCollector = new FirehoseCollector();
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockRecord);
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordCountEmptyList() {
        assertFalse(firehoseCollector.shouldCreateNewBatchDueToRecordCount());
    }

    @Test
    public void testShouldCreateNewBatchDueToDataSizeEmptyList() {
        when(mockRecord.getData()).thenReturn(FULL_DATA);

        assertFalse(firehoseCollector.shouldCreateNewBatchDueToDataSize(mockRecord));

        verify(mockRecord).getData();
    }

    @Test
    public void testShouldCreateNewBatchDueToDataSizeTooLarge() {
        when(mockRecord.getData()).thenReturn(FULL_DATA).thenReturn(FULL_DATA).thenReturn(ONE_BYTE_DATA);

        firehoseCollector.addRecordAndReturnBatch(mockRecord);
        firehoseCollector.addRecordAndReturnBatch(mockRecord);

        assertFalse(firehoseCollector.shouldCreateNewBatchDueToDataSize(mockRecord));
        verify(mockRecord, times(5)).getData();
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordCountRoomForJustOneMoreRecord() {
        testShouldCreateNewBatchDueToRecordCount(MAX_RECORDS_IN_BATCH - 2, false);
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordCountNoMoreRoom() {
        testShouldCreateNewBatchDueToRecordCount(MAX_RECORDS_IN_BATCH - 1, true);

        final List<Record> batch = firehoseCollector.addRecordAndReturnBatch(mockRecord);

        assertEquals(MAX_RECORDS_IN_BATCH, batch.size());
        verify(mockRecord, times(2 * MAX_RECORDS_IN_BATCH)).getData();
    }

    private void testShouldCreateNewBatchDueToRecordCount(int recordCount, boolean expected) {
        when(mockRecord.getData()).thenReturn(EMPTY_DATA);
        for (int i = 0; i < recordCount; i++) {
            final List<Record> batch = firehoseCollector.addRecordAndReturnBatch(mockRecord);
            assertTrue(batch.isEmpty());
        }

        assertEquals(expected, firehoseCollector.shouldCreateNewBatchDueToRecordCount());

        verify(mockRecord, times(2 * recordCount)).getData();
    }
}
