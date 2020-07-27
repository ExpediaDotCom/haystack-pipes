package com.expedia.www.haystack.pipes.kafka;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SPAN_ARRIVAL_TIMER_NAME;
import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SpringConfigTest {
    @Mock
    private MetricObjects mockMetricObjects;
    @Mock
    private Counter mockCounter;
    @Mock
    private Timer mockTimer;
    @Mock
    private HealthController mockHealthController;

    private SpringConfig springConfig;

    @Before
    public void setUp() {
        springConfig = new SpringConfig(mockMetricObjects);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockMetricObjects, mockCounter, mockHealthController);
    }

    @Test
    public void testProduceIntoExternalKafkaActionRequestCounter() {
        when(mockMetricObjects.createAndRegisterResettingCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        assertNotNull(springConfig.produceIntoExternalKafkaActionRequestCounter());

        verify(mockMetricObjects).createAndRegisterResettingCounter(SUBSYSTEM, Constants.APPLICATION,
                KafkaToKafkaPipeline.class.getSimpleName(), "REQUEST");
    }

    @Test
    public void testPostsInFlightCounter() {
        when(mockMetricObjects.createAndRegisterResettingCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        assertNotNull(springConfig.postsInFlightCounter());

        verify(mockMetricObjects).createAndRegisterResettingCounter(SUBSYSTEM, Constants.APPLICATION,
                KafkaToKafkaPipeline.class.getSimpleName(), "POSTS_IN_FLIGHT");
    }

    @Test
    public void testProduceIntoExternalKafkaCallbackLogger() {
        final Logger logger = springConfig.kafkaCallbackLogger();

        assertEquals(KafkaCallback.class.getName(), logger.getName());
    }

    @Test
    public void testProduceIntoExternalKafkaActionLogger() {
        final Logger logger = springConfig.kafkaToExternalKafkaActionLogger();

        assertEquals(KafkaToKafkaPipeline.class.getName(), logger.getName());
    }

    @Test
    public void testAppLogger() {
        final Logger logger = springConfig.appLogger();

        assertEquals(App.class.getName(), logger.getName());
    }

    @Test
    public void testKafkaStreamStarter() {
        final KafkaStreamStarter kafkaStreamStarter = springConfig.kafkaStreamStarter(mockHealthController);

        assertSame(ProtobufToKafkaProducer.class, kafkaStreamStarter.containingClass);
        assertSame(Constants.APPLICATION, kafkaStreamStarter.clientId);
    }

    @Test
    public void testKafkaProducerPost() {
        when(mockMetricObjects.createAndRegisterBasicTimer(
                anyString(), anyString(), anyString(), anyString(), any(TimeUnit.class)))
                .thenReturn(mockTimer);

        assertNotNull(springConfig.kafkaProducerPost());

        verify(mockMetricObjects).createAndRegisterBasicTimer(SUBSYSTEM, Constants.APPLICATION,
                KafkaToKafkaPipeline.class.getSimpleName(), "KAFKA_PRODUCER_POST", MICROSECONDS);
    }

    @Test
    public void testSpanArrivalTimer() {
        springConfig.spanArrivalTimer();

        verify(mockMetricObjects).createAndRegisterBasicTimer(SUBSYSTEM, Constants.APPLICATION,
                KafkaToKafkaPipeline.class.getSimpleName(), SPAN_ARRIVAL_TIMER_NAME, MILLISECONDS);
    }

}
