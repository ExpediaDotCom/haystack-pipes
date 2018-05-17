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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class FirehostStringBufferCollectorTest {
    private FirehoseCollector.Factory factory = new FirehoseCollector.Factory();

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testDefaultConstructorInitializedWithProvidedDefaults() {
        final FirehoseStringBufferCollector firehoseStringBufferCollector = new FirehoseStringBufferCollector();
        assertEquals(1000 * 1024, firehoseStringBufferCollector.getMaxBytesInRecord());
        assertEquals(4, firehoseStringBufferCollector.getMaxRecordsInBatch());
        assertEquals( 0, firehoseStringBufferCollector.getMaxBatchInterval());
    }

    @Test
    public void testExpectedBufferSizeWithDataAddsCurrentDataSizeWithBuffer() {
        final String data = "hello world!";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector();

        int actual = firehoseCollector.expectedBufferSizeWithData(data);
        assertEquals(data.length() + 1, actual);
        
        firehoseCollector.addRecordAndReturnBatch(data);
        actual = firehoseCollector.expectedBufferSizeWithData(data);
        assertEquals((2 * data.length()) + 1, actual);
    }

    @Test
    public void testBatchCreationTimeoutShouldReturnFalseIfDisabled() {
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(0);
        assertFalse(firehoseCollector.batchCreationTimedOut());
    }

    @Test
    public void testBatchCreationTimeoutShouldReturnFalseIfEnabledAndTimeHasNotElapsed() {
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(3000);
        assertFalse(firehoseCollector.batchCreationTimedOut());
    }

    @Test
    public void testBatchCreationTimeoutShouldReturnTrueIfEnabledAndTimeHasElapsed() throws InterruptedException {
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(10);
        Thread.sleep(20);
        assertTrue(firehoseCollector.batchCreationTimedOut());
    }

    @Test
    public void testInitializeBufferShouldInitializeBuffer() {
        final String data = "hello world!";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(10);
        firehoseCollector.addRecordAndReturnBatch(data);
        assertTrue(firehoseCollector.bufferSize() > 0);

        firehoseCollector.initializeBuffer();
        assertTrue(firehoseCollector.bufferSize() == 0);
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordSizeShouldReturnTrueIfDataSizeEqualsMaxRecordSize() {
        final String data = "hello world!";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 12, 1, 10);
        assertTrue(firehoseCollector.shouldCreateNewRecordDueToRecordSize(data));
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordSizeShouldReturnTrueIfExpectedDataSizeExceedsMaxRecordSize() {
        final String data = "hello world!";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 20, 1, 10);
        firehoseCollector.addRecordAndReturnBatch(data);
        assertTrue(firehoseCollector.shouldCreateNewRecordDueToRecordSize(data));
    }

    @Test
    public void testShouldCreateNewBatchDueToRecordSizeShouldReturnFalseIfExpectedDataSizeLessThanMaxRecordSize() {
        final String data = "hello world!";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 30, 1, 10);
        firehoseCollector.addRecordAndReturnBatch(data);
        assertFalse(firehoseCollector.shouldCreateNewRecordDueToRecordSize(data));
    }

    @Test
    public void testShouldDispatchBatchReturnsTrueIfBatchIsFull() {
        final String data = "Hello World!";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 12, 1, 10);
        firehoseCollector.addToRecordBuffer(data);
        assertTrue(firehoseCollector.addRecordAndReturnBatch(data) != null);
    }

    @Test
    public void testShouldDispatchBatchReturnsTrueIfBatchHasTimedOut() throws InterruptedException {
        final String data = "Hello World!";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 100, 1, 10);
        firehoseCollector.addToRecordBuffer(data);
        Thread.sleep(20);
        assertTrue(firehoseCollector.createNewBatchIfFull() != null);
    }

    @Test
    public void testCreateRecordFromBufferShouldReturnEmptyIfBufferIsEmpty() {
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 100, 1, 10);
        final Optional<Record> actual = firehoseCollector.createRecordFromBuffer();
        assertFalse(actual.isPresent());
    }

    @Test
    public void testCreateRecordFromBufferShouldReturnARecord() {
        final String data = "Hello World!";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 100, 1, 10);
        firehoseCollector.addRecordAndReturnBatch(data);
        final Optional<Record> actual = firehoseCollector.createRecordFromBuffer();
        assertTrue(actual.isPresent());
        assertEquals(data, new String(actual.get().getData().array()));
    }

    @Test
    public void testCreateNewBatchIfFullShouldReturnEmptyIfBatchIsNotFull() {
        final String data = "Hello World!";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 100, 1, 10);
        firehoseCollector.addRecordAndReturnBatch(data);
        assertTrue(firehoseCollector.createNewBatchIfFull().isEmpty());
    }

    @Test
    public void testAddToRecordBufferShouldAppendANewLineIfBufferHasData() {
        final String data = "Hello World!\n";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 100, 1, 10);
        firehoseCollector.addToRecordBuffer(data);
        firehoseCollector.addToRecordBuffer(data);
        assertEquals(26, firehoseCollector.getTotalBatchSize());
        final List<Record> batch = firehoseCollector.returnIncompleteBatch();
        final Record actual = batch.get(0);
        assertEquals("Hello World!\nHello World!\n", new String(actual.getData().array()));
    }

    @Test
    public void testAddingRecordsToABatchShouldReturnRightBatchSize() {
        final String data = "Hello World!\n";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 30, 3, 10);
        firehoseCollector.addRecordAndReturnBatch(data);
        firehoseCollector.addRecordAndReturnBatch(data);
        firehoseCollector.addRecordAndReturnBatch(data);
        firehoseCollector.addRecordAndReturnBatch(data);
        assertEquals(52, firehoseCollector.getTotalBatchSize());
        final List<Record> batch = firehoseCollector.returnIncompleteBatch();
        assertEquals("Hello World!\nHello World!\n", new String(batch.get(0).getData().array()));
        assertEquals("Hello World!\nHello World!\n", new String(batch.get(1).getData().array()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddingDataLargerThanRecordSizeShouldThrow() {
        final String data = "Hello World!";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 10, 3, 10);
        firehoseCollector.addRecordAndReturnBatch(data);
    }

    @Test
    public void testShouldReturnBatchOfRecordsWhenBufferIsFull() {
        final String data = "Hello World!\n";
        final FirehoseStringBufferCollector firehoseCollector = new FirehoseStringBufferCollector(factory, 40, 2, 10);
        List<Record> batch = Collections.emptyList();
        while (batch.isEmpty()) {
            batch = firehoseCollector.addRecordAndReturnBatch(data);
        }
        assertEquals(2, batch.size());
        assertEquals("Hello World!\nHello World!\nHello World!\n", new String(batch.get(0).getData().array()));
        assertEquals("Hello World!\nHello World!\nHello World!\n", new String(batch.get(1).getData().array()));
    }

}
