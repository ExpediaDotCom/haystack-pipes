package com.expedia.www.haystack.pipes.kafkaProducer;

import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CountersAndTimerTest {

    @Mock
    private Counter mockRequestCounter;

    @Mock
    private Counter mockPostsInFlightCounter;

    @Mock
    private Timer mockTimer;

    @Mock
    private Stopwatch mockStopwatch;

    private CountersAndTimer countersAndTimer;

    @Before
    public void setUp() {
        countersAndTimer = new CountersAndTimer(mockRequestCounter, mockPostsInFlightCounter, mockTimer);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockRequestCounter, mockPostsInFlightCounter, mockTimer);
        verifyNoMoreInteractions(mockStopwatch);
    }

    @Test
    public void testIncrementRequestCounter() {
        countersAndTimer.incrementRequestCounter();

        verify(mockRequestCounter).increment();
    }

    @Test
    public void testStartTimer() {
        when(mockTimer.start()).thenReturn(mockStopwatch);

        final Stopwatch stopwatch = countersAndTimer.startTimer();

        assertSame(mockStopwatch, stopwatch);
        verify(mockTimer).start();
    }

    @Test
    public void testIncrementPostsInFlightCounter() {
        countersAndTimer.incrementPostsInFlightCounter();

        verify(mockPostsInFlightCounter).increment();
    }
}
