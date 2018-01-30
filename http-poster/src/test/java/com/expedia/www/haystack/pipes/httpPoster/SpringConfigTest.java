package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.httpPoster.Constants.APPLICATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class SpringConfigTest {
    @Mock
    private MetricObjects mockMetricObjects;

    private SpringConfig springConfig;

    @Before
    public void setUp() {
        springConfig = new SpringConfig(mockMetricObjects);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockMetricObjects);
    }

    @Test
    public void testRequestCounter() {
        springConfig.requestCounter();

        verify(mockMetricObjects).createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                HttpPostAction.class.getName(), "REQUEST");
    }

    @Test
    public void testKafkaStreamStarter() {
        final KafkaStreamStarter kafkaStreamStarter = springConfig.kafkaStreamStarter();

        assertSame(ProtobufToHttpPoster.class, kafkaStreamStarter.containingClass);
        assertSame(APPLICATION, kafkaStreamStarter.clientId);
    }

    @Test
    public void testHttpPostIsActiveControllerLogger() {
        final Logger logger = springConfig.httpPostIsActiveControllerLogger();

        assertEquals(HttpPostIsActiveController.class.getName(), logger.getName());
    }

    // All of the other beans in SpringConfig use default constructors, or use arguments provided by other Spring beans
    // in SpringConfig, so tests on the methods that create those beans have little value.
}
