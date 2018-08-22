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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.STARTUP_MESSAGE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseProcessorTest {
    private static final String KEY = RANDOM.nextLong() + "KEY";
    private static final String STREAM_NAME = RANDOM.nextLong() + "STREAM_NAME";
    private static final long TIMESTAMP = System.currentTimeMillis();
    private static final Integer INITIAL_RETRY_SLEEP = 100;
    private static final Integer MAX_RETRY_SLEEP = 1000;

    @Mock
    private Logger mockLogger;
    @Mock
    private FirehoseCountersAndTimer mockFirehoseCountersAndTimer;
    @Mock
    private Batch mockBatch;
    @Mock
    private Factory mockFactory;
    @Mock
    private FirehoseConfigurationProvider mockFirehoseConfigurationProvider;
    @Mock
    private ProcessorContext mockProcessorContext;
    @Mock
    private Thread mockThread;
    @Mock
    private Runtime mockRuntime;
    @Mock
    private S3Sender mockS3Sender;
    @Mock
    private RetryCalculator mockRetryCalculator;
    @Mock
    private Record mockRecord;

    private FirehoseProcessor firehoseProcessor;
    private List<Record> recordList;

    @Before
    public void setUp() {
        when(mockFirehoseConfigurationProvider.streamname()).thenReturn(STREAM_NAME);
        firehoseProcessor = new FirehoseProcessor(mockLogger, mockFirehoseCountersAndTimer, () -> mockBatch,
                mockFirehoseConfigurationProvider, mockFactory, mockS3Sender);
        recordList = Collections.singletonList(mockRecord);
    }

    @After
    public void tearDown() {
        verify(mockLogger).info(String.format(STARTUP_MESSAGE, STREAM_NAME));
        verify(mockFirehoseConfigurationProvider).streamname();
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockFirehoseCountersAndTimer);
        verifyNoMoreInteractions(mockBatch);
        verifyNoMoreInteractions(mockFactory);
        verifyNoMoreInteractions(mockFirehoseConfigurationProvider);
        verifyNoMoreInteractions(mockProcessorContext);
        verifyNoMoreInteractions(mockThread);
        verifyNoMoreInteractions(mockRuntime);
        verifyNoMoreInteractions(mockS3Sender);
        verifyNoMoreInteractions(mockRetryCalculator);
        verifyNoMoreInteractions(mockRecord);
    }

    @Test
    public void testApplyHappyCase() throws InterruptedException {
        commonWhensForTestApply();

        firehoseProcessor.process(KEY, FULLY_POPULATED_SPAN);

        commonVerifiesForTestApply();
    }

    @Test
    public void testApplyInterruptedException() throws InterruptedException {
        commonWhensForTestApply();
        final InterruptedException interruptedException = new InterruptedException("Test");
        doThrow(interruptedException).when(mockS3Sender).sendRecordsToS3(anyListOf(Record.class), any());
        when(mockFactory.currentThread()).thenReturn(mockThread);

        firehoseProcessor.process(KEY, FULLY_POPULATED_SPAN);

        commonVerifiesForTestApply();
        verify(mockFactory).currentThread();
        verify(mockThread).interrupt();
    }

    private void commonWhensForTestApply() {
        when(mockFirehoseConfigurationProvider.initialretrysleep()).thenReturn(INITIAL_RETRY_SLEEP);
        when(mockFirehoseConfigurationProvider.maxretrysleep()).thenReturn(MAX_RETRY_SLEEP);
        when(mockFactory.createRetryCalculator(anyInt(), anyInt())).thenReturn(mockRetryCalculator);
        when(mockBatch.getRecordList(any())).thenReturn(recordList);
    }

    private void commonVerifiesForTestApply() throws InterruptedException {
        verify(mockFirehoseCountersAndTimer).incrementRequestCounter();
        verify(mockBatch).getRecordList(FULLY_POPULATED_SPAN);
        verify(mockFactory).createRetryCalculator(INITIAL_RETRY_SLEEP, MAX_RETRY_SLEEP);
        verify(mockFirehoseConfigurationProvider).initialretrysleep();
        verify(mockFirehoseConfigurationProvider).maxretrysleep();
        verify(mockFirehoseCountersAndTimer).recordSpanArrivalDelta(FULLY_POPULATED_SPAN);
        verify(mockS3Sender).sendRecordsToS3(recordList, mockRetryCalculator);
    }

    @Test
    public void testClose() {
        firehoseProcessor.close();

        verify(mockS3Sender).close();
    }

    @Test
    public void testInit() {
        when(mockFactory.createShutdownHook(any(FirehoseProcessor.class))).thenReturn(mockThread);
        when(mockFactory.getRuntime()).thenReturn(mockRuntime);
        firehoseProcessor.init(mockProcessorContext);
    }

    @Test
    public void testPunctuate() {
        firehoseProcessor.punctuate(TIMESTAMP);
    }

    @Test
    public void testSleeperSleep() throws InterruptedException {
        new Factory().createSleeper().sleep(0);
    }

}
