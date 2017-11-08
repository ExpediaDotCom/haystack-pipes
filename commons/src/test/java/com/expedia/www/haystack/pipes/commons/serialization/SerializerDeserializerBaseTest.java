package com.expedia.www.haystack.pipes.commons.serialization;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.Factory;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.BYTES_IN_COUNTERS;
import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.BYTES_IN_COUNTER_NAME;
import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.REQUESTS_COUNTERS;
import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.REQUEST_COUNTER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SerializerDeserializerBaseTest {
    private final static Random RANDOM = new Random();
    private final static String APPLICATION = RANDOM.nextLong() + "APPLICATION";
    private final static String CLASS_NAME = RANDOM.nextLong() + "CLASS_NAME";
    private final static String COUNTER_NAME = RANDOM.nextLong() + "COUNTER_NAME";
    private final static String TIMER_NAME = RANDOM.nextLong() + "TIMER_NAME";
    private static final String SIMPLE_NAME = SerializerDeserializerBase.class.getSimpleName();

    @Mock
    private MetricObjects mockMetricObjects;
    private MetricObjects realMetricObjects;

    @Mock
    private Counter mockCounter;

    @Mock
    private Timer mockTimer;

    @Mock
    private Factory mockFactory;
    private Factory realFactory;

    private SerializerDeserializerBase serializerDeserializerBase;

    @Before
    public void setUp() {
        realFactory = SerializerDeserializerBase.factory;
        SerializerDeserializerBase.factory = mockFactory;

        realMetricObjects = SerializerDeserializerBase.Factory.metricObjects;
        SerializerDeserializerBase.Factory.metricObjects = mockMetricObjects;

        when(mockFactory.createCounter(anyString(), anyString(), anyString())).thenReturn(mockCounter);
        serializerDeserializerBase = new SerializerDeserializerBase(APPLICATION);
    }

    @After
    public void tearDown() {
        SerializerDeserializerBase.factory = realFactory;
        SerializerDeserializerBase.Factory.metricObjects = realMetricObjects;

        REQUESTS_COUNTERS.clear();
        BYTES_IN_COUNTERS.clear();
        verify(mockFactory).createCounter(APPLICATION, SIMPLE_NAME, REQUEST_COUNTER_NAME);
        verify(mockFactory).createCounter(APPLICATION, SIMPLE_NAME, BYTES_IN_COUNTER_NAME);
        verifyNoMoreInteractions(mockMetricObjects, mockCounter, mockTimer, mockFactory);
    }

    @Test
    public void testFactoryCreateCounter() {
        when(mockMetricObjects.createAndRegisterCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        final Counter counter = realFactory.createCounter(APPLICATION, CLASS_NAME, COUNTER_NAME);

        assertSame(counter, mockCounter);
        verify(mockMetricObjects).createAndRegisterCounter(SUBSYSTEM, APPLICATION, CLASS_NAME, COUNTER_NAME);
    }

    @Test
    public void testFactoryCreateTimer() {
        when(mockMetricObjects.createAndRegisterBasicTimer(
                anyString(), anyString(), anyString(), anyString(), any(TimeUnit.class)))
                .thenReturn(mockTimer);

        final Timer timer = realFactory.createTimer(APPLICATION, CLASS_NAME, TIMER_NAME);

        assertSame(timer, mockTimer);
        verify(mockMetricObjects).createAndRegisterBasicTimer(
                SUBSYSTEM, APPLICATION, CLASS_NAME, TIMER_NAME, TimeUnit.MICROSECONDS);
    }

    @Test
    public void testGetOrCreateCounterThatWasAlreadyCreated() {
        when(mockFactory.createCounter(anyString(), anyString(), anyString())).thenReturn(mockCounter);
        final Map<String, Counter> counters = new HashMap<>();

        final Counter counter1 = serializerDeserializerBase.getOrCreateCounter(counters, COUNTER_NAME);
        final Counter counter2 = serializerDeserializerBase.getOrCreateCounter(counters, COUNTER_NAME);

        assertSame(counter1, counter2);
        assertEquals(1, counters.size());
        verify(mockFactory).createCounter(APPLICATION, SIMPLE_NAME, COUNTER_NAME);
    }

    @Test
    public void testGetOrCreateTimerThatWasAlreadyCreated() {
        when(mockFactory.createTimer(anyString(), anyString(), anyString())).thenReturn(mockTimer);
        final Map<String, Timer> timers = new HashMap<>();

        final Timer timer1 = serializerDeserializerBase.getOrCreateTimer(timers, TIMER_NAME);
        final Timer timer2 = serializerDeserializerBase.getOrCreateTimer(timers, TIMER_NAME);

        assertSame(timer1, timer2);
        assertEquals(1, timers.size());
        verify(mockFactory).createTimer(APPLICATION, SIMPLE_NAME, TIMER_NAME);
    }

    @Test(expected = OutOfMemoryError.class)
    public void testGetOrCreateCounterErrorCase() {
        when(mockFactory.createCounter(APPLICATION, SIMPLE_NAME, COUNTER_NAME)).thenThrow(new OutOfMemoryError());

        try {
            serializerDeserializerBase.getOrCreateCounter(new HashMap<>(), COUNTER_NAME);
        } finally {
            verify(mockFactory).createCounter(APPLICATION, SIMPLE_NAME, COUNTER_NAME);
        }
    }

    @Test(expected = OutOfMemoryError.class)
    public void testGetOrCreateTimerErrorCase() {
        when(mockFactory.createTimer(APPLICATION, SIMPLE_NAME, TIMER_NAME)).thenThrow(new OutOfMemoryError());

        try {
            serializerDeserializerBase.getOrCreateTimer(new HashMap<>(), TIMER_NAME);
        } finally {
            verify(mockFactory).createTimer(APPLICATION, SIMPLE_NAME, TIMER_NAME);
        }
    }
}
