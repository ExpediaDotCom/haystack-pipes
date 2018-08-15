package com.expedia.www.haystack.pipes.commons;

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
public class CountersAndTimerTest {
    private static final int VALUE = RANDOM.nextInt();
    private static final long DELTA = RANDOM.nextInt(Integer.MAX_VALUE);
    private static final long NOW = System.currentTimeMillis();

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

    private CountersAndTimer countersAndTimer;

    @Before
    public void setUp() {
        countersAndTimer = new CountersAndTimer(
                mockClock, mockTimer, mockSpanArrivalTimer, mockRequestCounter, mockSecondCounter);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockRequestCounter, mockSecondCounter, mockTimer);
        verifyNoMoreInteractions(mockStopwatch);
    }

    @Test
    public void testIncrementRequestCounter() {
        countersAndTimer.incrementRequestCounter();

        verify(mockRequestCounter).increment();
    }

    @Test
    public void testIncrementSecondCounter() {
        countersAndTimer.incrementCounter(0);

        verify(mockSecondCounter).increment();
    }

    @Test
    public void testIncrementSecondCounterWithValue() {
        countersAndTimer.incrementCounter(0, VALUE);

        verify(mockSecondCounter).increment(VALUE);
    }

    @Test
    public void testStartTimer() {
        when(mockTimer.start()).thenReturn(mockStopwatch);

        final Stopwatch stopwatch = countersAndTimer.startTimer();

        assertSame(mockStopwatch, stopwatch);
        verify(mockTimer).start();
    }


    @Test
    public void testRecordSpanArrivalDelta() {
        when(mockClock.millis()).thenReturn(NOW);

        countersAndTimer.recordSpanArrivalDelta(NOW - DELTA);

        verify(mockClock).millis();
        verify(mockSpanArrivalTimer).record(DELTA, TimeUnit.MILLISECONDS);
    }

}
