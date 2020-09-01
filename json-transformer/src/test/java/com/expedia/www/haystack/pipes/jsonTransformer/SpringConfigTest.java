package com.expedia.www.haystack.pipes.jsonTransformer;

import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.HealthStatusListener;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.jsonTransformer.Constants.APPLICATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SpringConfigTest {

    @Mock
    private HealthController mockHealthController;

    @Mock
    private KafkaConfigurationProvider mockKafkaConfigurationProvider;

    private SpringConfig springConfig;

    @Before
    public void setUp() {
        springConfig = new SpringConfig();
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockHealthController);
    }

    @Test
    public void testJsonTransformerIsActiveControllerLogger() {
        final Logger logger = springConfig.jsonTransformerIsActiveControllerLogger();

        assertEquals(JsonTransformerIsActiveController.class.getName(), logger.getName());
    }

    @Test
    public void testKafkaStreamStarter() {
        final KafkaStreamStarter kafkaStreamStarter = springConfig.kafkaStreamStarter(mockHealthController,mockKafkaConfigurationProvider);

        assertSame(ProtobufToJsonTransformer.class, kafkaStreamStarter.containingClass);
        assertSame(APPLICATION, kafkaStreamStarter.clientId);
    }

    // All of the other beans in SpringConfig use default constructors, or use arguments provided by other Spring beans
    // in SpringConfig, so tests on the methods that create those beans have little value.
}
