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
package com.expedia.www.haystack.pipes.commons;

import com.expedia.open.tracing.SpanOrBuilder;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TimersAndCountersTest {
    private static final int VALUE = RANDOM.nextInt();
    private static final long DELTA_MILLIS = RANDOM.nextInt(Integer.MAX_VALUE);
    private static final long NOW_MILLIS = System.currentTimeMillis();

    @Mock
    private Counter mockRequestCounter;

    @Mock
    private Counter mockSecondCounter;

    @Mock
    private Timer mockTimer;

    @Mock
    private Stopwatch mockStopwatch;

    @Mock
    private Clock mockClock;

    @Mock
    private Timer mockSpanArrivalTimer;

    @Mock
    private SpanOrBuilder mockSpanOrBuilder;

    private TimersAndCounters timersAndCounters;

    @Before
    public void setUp() {
        final Timers timers = new Timers(mockTimer, mockSpanArrivalTimer);
        timersAndCounters = new TimersAndCounters(
                mockClock, timers, mockRequestCounter, mockSecondCounter);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockRequestCounter, mockSecondCounter, mockTimer);
        verifyNoMoreInteractions(mockStopwatch, mockClock, mockSpanArrivalTimer, mockSpanOrBuilder);
    }

    @Test
    public void testIncrementRequestCounter() {
        timersAndCounters.incrementRequestCounter();

        verify(mockRequestCounter).increment();
    }

    @Test
    public void testIncrementSecondCounter() {
        timersAndCounters.incrementCounter(0);

        verify(mockSecondCounter).increment();
    }

    @Test
    public void testIncrementSecondCounterWithValue() {
        timersAndCounters.incrementCounter(0, VALUE);

        verify(mockSecondCounter).increment(VALUE);
    }

    @Test
    public void testStartTimer() {
        when(mockTimer.start()).thenReturn(mockStopwatch);

        final Stopwatch stopwatch = timersAndCounters.startTimer();

        assertSame(mockStopwatch, stopwatch);
        verify(mockTimer).start();
    }


    @Test
    public void testRecordSpanArrivalDelta() {
        testRecordSpanArrivalDelta((NOW_MILLIS - DELTA_MILLIS) * 1000L, DELTA_MILLIS * 1000L);
        verifiesForRecordSpanArrivalDeltaRecordsMetric(0L);
    }

    @Test
    public void testRecordSpanArrivalDeltaSpanArrivalTimeMillis1() {
        testRecordSpanArrivalDelta(0L, 1000L);
        verifiesForRecordSpanArrivalDeltaRecordsMetric(NOW_MILLIS - 1L);
    }

    @Test
    public void testRecordSpanArrivalDeltaSpanArrivalTimeMillis0() {
        testRecordSpanArrivalDelta(0L, 0L);
    }

    private void testRecordSpanArrivalDelta(long startTimeMicros, long spanDurationMicros) {
        when(mockClock.millis()).thenReturn(NOW_MILLIS);
        when(mockSpanOrBuilder.getStartTime()).thenReturn(startTimeMicros);
        when(mockSpanOrBuilder.getDuration()).thenReturn(spanDurationMicros);

        timersAndCounters.recordSpanArrivalDelta(mockSpanOrBuilder);

        verify(mockSpanOrBuilder).getStartTime();
        verify(mockSpanOrBuilder).getDuration();
    }

    private void verifiesForRecordSpanArrivalDeltaRecordsMetric(long duration) {
        verify(mockClock).millis();
        verify(mockSpanArrivalTimer).record(duration, TimeUnit.MILLISECONDS);
    }
}
