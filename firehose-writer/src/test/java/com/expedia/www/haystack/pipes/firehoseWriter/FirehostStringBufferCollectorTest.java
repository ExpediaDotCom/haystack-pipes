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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.amazonaws.services.kinesisfirehose.model.Record;

import com.expedia.www.haystack.pipes.firehoseWriter.FirehoseCollector.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseCollector.MAX_BYTES_IN_RECORD;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseStringBufferCollector.MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirehostStringBufferCollectorTest {
    private static final String DATA = "Hello World!";
    private static final String DATA_PLUS_NEWLINE = DATA + '\n';

    private static final long NOW = System.currentTimeMillis();
    private static final int MAX_BATCH_INTERVAL = 10;

    @Mock
    private Factory mockFactory;

    private Factory realFactory = new Factory();

    private FirehoseStringBufferCollector firehoseStringBufferCollector;

    @Before
    public void setUp() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector();
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockFactory);
    }

    @Test
    public void testDefaultConstructorInitializedWithProvidedDefaults() {
        assertEquals(1000 * 1000, firehoseStringBufferCollector.getMaxBytesInRecord());
        assertEquals(MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR,
                firehoseStringBufferCollector.getMaxRecordsInBatch());
        assertEquals( 0, firehoseStringBufferCollector.getMaxBatchInterval());
    }

    @Test
    public void testExpectedBufferSizeWithDataAddsCurrentDataSizeWithBuffer() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector();

        int actual = firehoseStringBufferCollector.expectedBufferSizeWithData(DATA);
        assertEquals(DATA.length() + 1, actual);
        
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA);
        actual = firehoseStringBufferCollector.expectedBufferSizeWithData(DATA);
        assertEquals((2 * DATA.length()) + 1, actual);
    }

    @Test
    public void testBatchCreationTimeoutShouldReturnFalseIfDisabled() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(0);
        assertFalse(firehoseStringBufferCollector.batchCreationTimedOut());
    }

    @Test
    public void testBatchCreationTimeoutShouldReturnFalseIfEnabledAndTimeHasNotElapsed() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(3000);
        assertFalse(firehoseStringBufferCollector.batchCreationTimedOut());
    }

    @Test
    public void testBatchCreationTimeoutShouldReturnTrueIfEnabledAndTimeHasElapsed() {
        when(mockFactory.currentTimeMillis()).thenReturn(NOW, NOW + MAX_BATCH_INTERVAL + 1);
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                mockFactory, MAX_BYTES_IN_RECORD, MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR, MAX_BATCH_INTERVAL);
        assertTrue(firehoseStringBufferCollector.batchCreationTimedOut());
        verify(mockFactory, times(2)).currentTimeMillis();
    }

    @Test
    public void testBatchCreationTimeoutShouldReturnTrueIfEnabledAndTimeHasNotQuiteElapsed() {
        when(mockFactory.currentTimeMillis()).thenReturn(NOW, NOW + MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                mockFactory, MAX_BYTES_IN_RECORD, MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR, MAX_BATCH_INTERVAL);
        assertFalse(firehoseStringBufferCollector.batchCreationTimedOut());
        verify(mockFactory, times(2)).currentTimeMillis();
    }

    @Test
    public void testInitializeBufferShouldInitializeBuffer() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(10);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA);
        assertTrue(firehoseStringBufferCollector.bufferSize() > 0);

        firehoseStringBufferCollector.initializeBuffer();
        assertEquals(0, firehoseStringBufferCollector.bufferSize());
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordSizeShouldReturnTrueIfDataSizeEqualsMaxRecordSize() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 12, 1, MAX_BATCH_INTERVAL);
        assertTrue(firehoseStringBufferCollector.shouldCreateNewRecordDueToRecordSize(DATA));
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordSizeShouldReturnTrueIfExpectedDataSizeExceedsMaxRecordSize() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 20, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA);
        assertTrue(firehoseStringBufferCollector.shouldCreateNewRecordDueToRecordSize(DATA));
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordSizeShouldReturnFalseIfExpectedDataSizeLessThanMaxRecordSize() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 30, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA);
        assertFalse(firehoseStringBufferCollector.shouldCreateNewRecordDueToRecordSize(DATA));
    }

    @Test
    public void testAddRecordAndReturnBatchReturnsNonEmptyBatchIfBatchIsNotFull() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 12, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addToRecordBuffer(DATA);

        final List<Record> records = firehoseStringBufferCollector.addRecordAndReturnBatch(DATA);

        assertEquals(1, records.size());
    }

    @Test
    public void testCreateNewBatchIfFullReturnsNonEmptyBatchIfBatchHasTimedOut() {
        when(mockFactory.currentTimeMillis()).thenReturn(NOW, NOW + MAX_BATCH_INTERVAL + 1);
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(mockFactory, 100, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addToRecordBuffer(DATA);

        final List<Record> records = firehoseStringBufferCollector.createNewBatchIfFull();

        assertEquals(1, records.size());
        verify(mockFactory, times(3)).currentTimeMillis();
    }

    @Test
    public void testCreateRecordFromBufferShouldReturnEmptyIfBufferIsEmpty() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 100, 1, MAX_BATCH_INTERVAL);
        final Optional<Record> actual = firehoseStringBufferCollector.createRecordFromBuffer();
        assertFalse(actual.isPresent());
    }

    @Test
    public void testCreateRecordFromBufferShouldReturnARecord() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 100, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA);
        final Optional<Record> actual = firehoseStringBufferCollector.createRecordFromBuffer();
        assertTrue(actual.isPresent());
        assertEquals(DATA, new String(actual.get().getData().array()));
    }

    @Test
    public void testCreateNewBatchIfFullShouldReturnEmptyIfBatchIsNotFull() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 100, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA);
        assertTrue(firehoseStringBufferCollector.createNewBatchIfFull().isEmpty());
    }

    @Test
    public void testAddToRecordBufferShouldAppendANewLineIfBufferHasData() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 100, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addToRecordBuffer(DATA_PLUS_NEWLINE);
        firehoseStringBufferCollector.addToRecordBuffer(DATA_PLUS_NEWLINE);
        assertEquals(26, firehoseStringBufferCollector.getTotalBatchSize());
        final List<Record> batch = firehoseStringBufferCollector.returnIncompleteBatch();
        final Record actual = batch.get(0);
        assertEquals(DATA_PLUS_NEWLINE + DATA_PLUS_NEWLINE, new String(actual.getData().array()));
    }

    @Test
    public void testAddingRecordsToABatchShouldReturnRightBatchSize() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 30, 3, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_PLUS_NEWLINE);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_PLUS_NEWLINE);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_PLUS_NEWLINE);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_PLUS_NEWLINE);
        assertEquals(52, firehoseStringBufferCollector.getTotalBatchSize());
        final List<Record> batch = firehoseStringBufferCollector.returnIncompleteBatch();
        final String expected = DATA_PLUS_NEWLINE + DATA_PLUS_NEWLINE;
        assertEquals(expected, new String(batch.get(0).getData().array()));
        assertEquals(expected, new String(batch.get(1).getData().array()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddingDataLargerThanRecordSizeShouldThrow() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 10, 3, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA);
    }

    @Test
    public void testShouldReturnBatchOfRecordsWhenBufferIsFull() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(realFactory, 40, 2, MAX_BATCH_INTERVAL);
        List<Record> batch = Collections.emptyList();
        while (batch.isEmpty()) {
            batch = firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_PLUS_NEWLINE);
        }
        assertEquals(2, batch.size());
        final String expected = DATA_PLUS_NEWLINE + DATA_PLUS_NEWLINE + DATA_PLUS_NEWLINE;
        assertEquals(expected, new String(batch.get(0).getData().array()));
        assertEquals(expected, new String(batch.get(1).getData().array()));
    }

}
