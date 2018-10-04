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
import com.expedia.www.haystack.pipes.firehoseWriter.FirehoseCollector.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseCollector.MAX_BYTES_IN_BATCH;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseCollector.MAX_BYTES_IN_RECORD;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseStringBufferCollector.MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseStringBufferCollectorTest {
    private static final String HELLO_WORLD = "Hello World!\u1234"; // include non-ASCII character
    private static final byte[] DATA = HELLO_WORLD.getBytes(StandardCharsets.UTF_8);
    private static final String DATA_STRING = new String(DATA);
    private static final String DATA_PLUS_NEWLINE_STRING = HELLO_WORLD + '\n';
    private static final byte[] DATA_PLUS_NEWLINE = DATA_PLUS_NEWLINE_STRING.getBytes(StandardCharsets.UTF_8);

    private static final long NOW = System.currentTimeMillis();
    private static final int MAX_BATCH_INTERVAL = 10;

    @Mock
    private Logger mockLogger;

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
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockFactory);
    }

    @Test
    public void testDefaultConstructorInitializedWithProvidedDefaults() {
        assertEquals(999 * 1000, firehoseStringBufferCollector.getMaxBytesInRecord());
        assertEquals(MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR,
                firehoseStringBufferCollector.getMaxRecordsInBatch());
        assertEquals( 0, firehoseStringBufferCollector.getMaxBatchInterval());
    }

    @Test
    public void testExpectedBufferSizeWithDataAddsCurrentDataSizeWithBuffer() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector();

        int actual = firehoseStringBufferCollector.expectedBufferSizeWithData(DATA);
        assertEquals(DATA.length + 1, actual);
        
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_STRING);
        actual = firehoseStringBufferCollector.expectedBufferSizeWithData(DATA);
        assertEquals((2 * DATA.length) + 1, actual);
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
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(mockFactory, MAX_BYTES_IN_RECORD,
                MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR, MAX_BATCH_INTERVAL);
        assertTrue(firehoseStringBufferCollector.batchCreationTimedOut());
        verify(mockFactory, times(2)).currentTimeMillis();
    }

    @Test
    public void testBatchCreationTimeoutShouldReturnTrueIfEnabledAndTimeHasNotQuiteElapsed() {
        when(mockFactory.currentTimeMillis()).thenReturn(NOW, NOW + MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(mockFactory, MAX_BYTES_IN_RECORD,
                MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR, MAX_BATCH_INTERVAL);
        assertFalse(firehoseStringBufferCollector.batchCreationTimedOut());
        verify(mockFactory, times(2)).currentTimeMillis();
    }

    @Test
    public void testInitializeBufferShouldInitializeBuffer() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(10);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_STRING);
        assertTrue(firehoseStringBufferCollector.bufferIndex > 0);

        firehoseStringBufferCollector.initializeBuffer();
        assertEquals(0, firehoseStringBufferCollector.bufferIndex);
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordSizeShouldReturnTrueIfDataSizeEqualsMaxRecordSize() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 12, 1, MAX_BATCH_INTERVAL);
        assertTrue(firehoseStringBufferCollector.shouldCreateNewRecordDueToRecordSize(DATA));
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordSizeShouldReturnTrueIfExpectedDataSizeExceedsMaxRecordSize() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 20, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_STRING);
        assertTrue(firehoseStringBufferCollector.shouldCreateNewRecordDueToRecordSize(DATA));
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordSizeShouldReturnFalseIfExpectedDataSizeLessThanMaxRecordSize() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 36, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_STRING);
        assertFalse(firehoseStringBufferCollector.shouldCreateNewRecordDueToRecordSize(DATA));
    }

    @Test
    public void testAddRecordAndReturnBatchReturnsNonEmptyBatchIfBatchIsNotFull() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 15, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addToRecordBuffer(DATA);

        final List<Record> records = firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_STRING);

        assertEquals(1, records.size());
    }

    @Test
    public void testCreateNewBatchIfFullReturnsNonEmptyBatchIfBatchHasTimedOut() {
        when(mockFactory.currentTimeMillis()).thenReturn(NOW, NOW + MAX_BATCH_INTERVAL + 1);
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                mockFactory, 100, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addToRecordBuffer(DATA);

        final List<Record> records = firehoseStringBufferCollector.createNewBatchIfFull();

        assertEquals(1, records.size());
        verify(mockFactory, times(3)).currentTimeMillis();
    }

    @Test
    public void testCreateRecordFromBufferShouldReturnEmptyIfBufferIsEmpty() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 100, 1, MAX_BATCH_INTERVAL);
        final Optional<Record> actual = firehoseStringBufferCollector.createRecordFromBuffer();
        assertFalse(actual.isPresent());
    }

    @Test
    public void testCreateRecordFromBufferShouldReturnARecord() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 100, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_STRING);
        final Optional<Record> actual = firehoseStringBufferCollector.createRecordFromBuffer();
        assertTrue(actual.isPresent());
        assertArrayEquals(DATA, actual.get().getData().array());
    }

    @Test
    public void testCreateNewBatchIfFullShouldReturnEmptyIfBatchIsNotFull() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 100, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_STRING);
        assertTrue(firehoseStringBufferCollector.createNewBatchIfFull().isEmpty());
    }

    @Test
    public void testAddToRecordBufferShouldAppendANewLineIfBufferHasData() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 100, 1, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addToRecordBuffer(DATA_PLUS_NEWLINE);
        firehoseStringBufferCollector.addToRecordBuffer(DATA_PLUS_NEWLINE);
        assertEquals(32, firehoseStringBufferCollector.getTotalBatchSize());
        final List<Record> batch = firehoseStringBufferCollector.createIncompleteBatch();
        final Record actual = batch.get(0);
        assertEquals(DATA_PLUS_NEWLINE_STRING + DATA_PLUS_NEWLINE_STRING, new String(actual.getData().array()));
    }

    @Test
    public void testAddRecordToBufferBoundaryCaseRealLimits() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(mockFactory, MAX_BYTES_IN_RECORD,
                MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR, MAX_BATCH_INTERVAL);
        fillFirstThreeRecords();
        fillFourthRecordToTheBrim();
        final int recordSizeForLastRecord = MAX_BYTES_IN_BATCH - firehoseStringBufferCollector.getTotalBatchSize();
        final String dataForLastRecord = createStringOfSize(recordSizeForLastRecord);
        assertFalse(firehoseStringBufferCollector.addRecordAndReturnBatch(dataForLastRecord).isEmpty());
        assertEquals(recordSizeForLastRecord, firehoseStringBufferCollector.getTotalBatchSize());
        verify(mockFactory, times(MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR + 2))
                .currentTimeMillis();
    }

    private void fillFirstThreeRecords() {
        final String dataThatConsumesTheEntireBatch = createStringOfSize(MAX_BYTES_IN_RECORD);
        for(int i = 0 ; i < MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR - 1 ; i++) {
            assertTrue(firehoseStringBufferCollector.addRecordAndReturnBatch(dataThatConsumesTheEntireBatch).isEmpty());
        }
    }

    private void fillFourthRecordToTheBrim() {
        final String dataForAllRecordsButLast = createStringOfSize(MAX_BYTES_IN_RECORD - 1);
        assertTrue(firehoseStringBufferCollector.addRecordAndReturnBatch(dataForAllRecordsButLast).isEmpty());
    }

    private String createStringOfSize(int size) {
        final StringBuilder stringBuilder = new StringBuilder(size);
        for(int i = 0 ; i < size ; i++) {
            stringBuilder.append("!");
        }
        return stringBuilder.toString();
    }

    @Test
    public void testAddingRecordsToABatchShouldReturnRightBatchSize() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 36, 3, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_PLUS_NEWLINE_STRING);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_PLUS_NEWLINE_STRING);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_PLUS_NEWLINE_STRING);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_PLUS_NEWLINE_STRING);
        assertEquals(64, firehoseStringBufferCollector.getTotalBatchSize());
        final List<Record> batch = firehoseStringBufferCollector.createIncompleteBatch();
        final String expected = DATA_PLUS_NEWLINE_STRING + DATA_PLUS_NEWLINE_STRING;
        assertEquals(expected, new String(batch.get(0).getData().array()));
        assertEquals(expected, new String(batch.get(1).getData().array()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddingDataLargerThanRecordSizeShouldThrow() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 10, 3, MAX_BATCH_INTERVAL);
        firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_STRING);
    }

    @Test
    public void testShouldReturnBatchOfRecordsWhenBufferIsFull() {
        firehoseStringBufferCollector = new FirehoseStringBufferCollector(
                realFactory, 49, 2, MAX_BATCH_INTERVAL);
        List<Record> batch = Collections.emptyList();
        while (batch.isEmpty()) {
            batch = firehoseStringBufferCollector.addRecordAndReturnBatch(DATA_PLUS_NEWLINE_STRING);
        }
        assertEquals(2, batch.size());
        final String expected = DATA_PLUS_NEWLINE_STRING + DATA_PLUS_NEWLINE_STRING + DATA_PLUS_NEWLINE_STRING;
        assertEquals(expected, new String(batch.get(0).getData().array()));
        assertEquals(expected, new String(batch.get(1).getData().array()));
    }

}
