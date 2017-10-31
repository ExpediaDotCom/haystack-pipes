package com.expedia.www.haystack.pipes.commons;

import com.netflix.servo.monitor.Counter;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.SerializerDeserializerBase.Factory;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.commons.SerializerDeserializerBase.metricObjects;
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
