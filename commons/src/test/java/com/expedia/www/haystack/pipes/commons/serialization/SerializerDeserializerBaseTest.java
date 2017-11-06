package com.expedia.www.haystack.pipes.commons.serialization;

import com.netflix.servo.monitor.Counter;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.Factory;
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
import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.BYTES_IN_COUNTER_NAME;
import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.REQUEST_COUNTER_NAME;
import static com.expedia.www.haystack.pipes.commons.serialization.SerializerDeserializerBase.metricObjects;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
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

    @Mock
    private MetricObjects mockMetricObjects;
    private MetricObjects realMetricObjects;

    @Mock
    private Counter mockCounter;

    @Mock
    private Timer mockTimer;

    private Factory factory;


    @Before
    public void setUp() {
        factory = new Factory();
        realMetricObjects = metricObjects;
        metricObjects = mockMetricObjects;
    }

    @After
    public void tearDown() {
        metricObjects = realMetricObjects;
        verifyNoMoreInteractions(mockMetricObjects, mockCounter, mockTimer);
    }

    @Test
    public void testCreateCounter() {
        when(mockMetricObjects.createAndRegisterCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        final Counter counter = factory.createCounter(APPLICATION, CLASS_NAME, COUNTER_NAME);

        assertSame(counter, mockCounter);
        verify(mockMetricObjects).createAndRegisterCounter(SUBSYSTEM, APPLICATION, CLASS_NAME, COUNTER_NAME);
    }

    @Test
    public void testGetOrCreateTimerThatWasAlreadyCreated() {
        when(mockMetricObjects.createAndRegisterCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);
        when(mockMetricObjects.createAndRegisterBasicTimer(
                anyString(), anyString(), anyString(), anyString(), any(TimeUnit.class)))
                .thenReturn(mockTimer).thenReturn(null); // thenReturn(null) should never happen
        final SerializerDeserializerBase serializerDeserializerBase;
        serializerDeserializerBase = new SerializerDeserializerBase(APPLICATION);

        final Map<String, Timer> timers = new HashMap<>();
        final Timer timer1 = serializerDeserializerBase.getOrCreateTimer(timers, TIMER_NAME);
        final Timer timer2 = serializerDeserializerBase.getOrCreateTimer(timers, TIMER_NAME);

        assertSame(timer1, timer2);
        final String sdbClassName = SerializerDeserializerBase.class.getSimpleName();
        verify(mockMetricObjects).createAndRegisterCounter(SUBSYSTEM, APPLICATION, sdbClassName, REQUEST_COUNTER_NAME);
        verify(mockMetricObjects).createAndRegisterCounter(SUBSYSTEM, APPLICATION, sdbClassName, BYTES_IN_COUNTER_NAME);
        verify(mockMetricObjects).createAndRegisterBasicTimer(
                SUBSYSTEM, APPLICATION, sdbClassName, TIMER_NAME, TimeUnit.MICROSECONDS);
    }

    @Test
    public void testCreateTimer() {
        when(mockMetricObjects.createAndRegisterBasicTimer(
                anyString(), anyString(), anyString(), anyString(), any(TimeUnit.class)))
                .thenReturn(mockTimer);

        final Timer timer = factory.createTimer(APPLICATION, CLASS_NAME, TIMER_NAME);

        assertSame(timer, mockTimer);
        verify(mockMetricObjects).createAndRegisterBasicTimer(
                SUBSYSTEM, APPLICATION, CLASS_NAME, TIMER_NAME, TimeUnit.MICROSECONDS);
    }
}
