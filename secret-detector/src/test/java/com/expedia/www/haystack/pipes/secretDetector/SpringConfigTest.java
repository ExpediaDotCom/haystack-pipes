package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.HealthStatusListener;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import io.dataapps.chlorine.finder.FinderEngine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus.HEALTHY;
import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
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
    @Mock
    private HealthStatusListener mockHealthStatusListener;
    @Mock
    private CountersAndTimer mockCountersAndTimer;
    @Mock
    private Detector mockDetector;
    @Mock
    private Logger mockLogger;
    @Mock
    private FinderEngine mockFinderEngine;

    private SpringConfig springConfig;

    @Before
    public void setUp() {
        springConfig = new SpringConfig(mockMetricObjects);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockMetricObjects, mockCounter, mockTimer, mockHealthController,
                mockHealthStatusListener, mockCountersAndTimer, mockDetector, mockLogger, mockFinderEngine);
    }

    @Test
    public void testDetectorIsActiveControllerLogger() {
        final Logger logger = springConfig.detectorIsActiveControllerLogger();

        assertEquals(DetectorIsActiveController.class.getName(), logger.getName());
    }

    @Test
    public void testKafkaStreamStarter() {
        final KafkaStreamStarter kafkaStreamStarter = springConfig.kafkaStreamStarter(mockHealthController);

        assertSame(DetectorProducer.class, kafkaStreamStarter.containingClass);
        assertSame(APPLICATION, kafkaStreamStarter.clientId);
    }

    @Test
    public void testHealthController() {
        final HealthController healthController = springConfig.healthController(mockHealthStatusListener);

        healthController.setHealthy();
        verify(mockHealthStatusListener).onChange(HEALTHY);
    }

    @Test
    public void testHealthStatusListener() {
        assertNotNull(springConfig.healthStatusListener());
    }

    @Test
    public void testDetectorActionRequestCounter() {
        when(mockMetricObjects.createAndRegisterResettingCounter(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockCounter);

        assertNotNull(springConfig.detectorActionRequestCounter());

        verify(mockMetricObjects).createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                DetectorAction.class.getSimpleName(), "DETECTOR_SPAN");
    }


    @Test
    public void testDetectorDetectTimer() {
        when(mockMetricObjects.createAndRegisterBasicTimer(
                anyString(), anyString(), anyString(), anyString(), any(TimeUnit.class)))
                .thenReturn(mockTimer);

        assertNotNull(springConfig.detectorDetectTimer());

        verify(mockMetricObjects).createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                DetectorAction.class.getSimpleName(), "DETECTOR_DETECT", MICROSECONDS);
    }

    @Test
    public void testCountersAndTimer() {
        assertNotNull(springConfig.countersAndTimer(mockCounter, mockTimer));
    }

    @Test
    public void testDetectorAction() {
        assertNotNull(springConfig.detectorAction(mockCountersAndTimer, mockDetector, mockLogger));
    }

    @Test
    public void testDetector() {
        assertNotNull(springConfig.detector(mockFinderEngine));
    }

    @Test
    public void testSpanSerdeFactory() {
        assertNotNull(springConfig.spanSerdeFactory());
    }

    @Test
    public void testKafkaConfigurationProvider() {
        assertNotNull(springConfig.kafkaConfigurationProvider());
    }

    @Test
    public void testKafkaProducerIsActiveControllerFactory() {
        assertNotNull(springConfig.kafkaProducerIsActiveControllerFactory());
    }

    @Test
    public void testFinderEngine() {
        assertNotNull(springConfig.finderEngine());
    }
}
