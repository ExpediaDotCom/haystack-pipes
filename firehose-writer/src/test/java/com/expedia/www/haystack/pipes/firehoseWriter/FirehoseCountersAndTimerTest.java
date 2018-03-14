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
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseCountersAndTimerTest {
    private static final int FAILURES = RANDOM.nextInt(Byte.MAX_VALUE);
    private static final int SUCCESSES = RANDOM.nextInt(Byte.MAX_VALUE);
    private static final int SIZE = FAILURES + SUCCESSES;

    @Mock
    private Timer mockTimer;
    @Mock
    private Counter mockSpanCounter;
    @Mock
    private Counter mockSuccessCounter;
    @Mock
    private Counter mockFailureCounter;
    @Mock
    private Counter mockExceptionCounter;
    @Mock
    private PutRecordBatchRequest mockRequest;
    @Mock
    private PutRecordBatchResult mockResult;
    @Mock
    private List<Record> mockRecordList;

    private FirehoseCountersAndTimer firehoseCountersAndTimer;

    @Before
    public void setUp() {
        firehoseCountersAndTimer = new FirehoseCountersAndTimer(
                mockTimer, mockSpanCounter, mockSuccessCounter, mockFailureCounter, mockExceptionCounter);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockSpanCounter, mockSuccessCounter, mockFailureCounter, mockExceptionCounter,
                mockRequest, mockResult, mockRecordList);
    }

    @Test
    public void testCountersCountSuccessesAndFailuresNullResult() {
        commonWhensForTestCountersCountSuccessesAndFailures();

        firehoseCountersAndTimer.countSuccessesAndFailures(mockRequest, null);

        commonVerifiesForTestCountersCountSuccessesAndFailures(0, SIZE);
    }

    @Test
    public void testCountersCountSuccessesAndFailuresNonNullResult() {
        commonWhensForTestCountersCountSuccessesAndFailures();
        when(mockResult.getFailedPutCount()).thenReturn(FAILURES);

        firehoseCountersAndTimer.countSuccessesAndFailures(mockRequest, mockResult);

        commonVerifiesForTestCountersCountSuccessesAndFailures(SUCCESSES, FAILURES);
        verify(mockResult).getFailedPutCount();
    }

    private void commonWhensForTestCountersCountSuccessesAndFailures() {
        when(mockRequest.getRecords()).thenReturn(mockRecordList);
        when(mockRecordList.size()).thenReturn(SIZE);
    }

    private void commonVerifiesForTestCountersCountSuccessesAndFailures(int successes, int failures) {
        verify(mockRequest).getRecords();
        verify(mockRecordList).size();
        verify(mockSuccessCounter).increment(successes);
        verify(mockFailureCounter).increment(failures);
    }

    @Test
    public void testCountersIncrementSpanCounter() {
        firehoseCountersAndTimer.incrementRequestCounter();

        verify(mockSpanCounter).increment();
    }

    @Test
    public void testCounterIncrementExceptionCounter() {
        firehoseCountersAndTimer.incrementExceptionCounter();

        verify(mockExceptionCounter).increment();
    }
}
