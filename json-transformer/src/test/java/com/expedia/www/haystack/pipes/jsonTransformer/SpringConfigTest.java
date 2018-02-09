package com.expedia.www.haystack.pipes.jsonTransformer;

import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.jsonTransformer.Constants.APPLICATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class SpringConfigTest {
    private SpringConfig springConfig;

    @Before
    public void setUp() {
        springConfig = new SpringConfig();
    }

    @Test
    public void testJsonTransformerIsActiveControllerLogger() {
        final Logger logger = springConfig.jsonTransformerIsActiveControllerLogger();

        assertEquals(JsonTransformerIsActiveController.class.getName(), logger.getName());
    }

    @Test
    public void testKafkaStreamStarter() {
        final KafkaStreamStarter kafkaStreamStarter = springConfig.kafkaStreamStarter();

        assertSame(ProtobufToJsonTransformer.class, kafkaStreamStarter.containingClass);
        assertSame(APPLICATION, kafkaStreamStarter.clientId);
    }

    // All of the other beans in SpringConfig use default constructors, or use arguments provided by other Spring beans
    // in SpringConfig, so tests on the methods that create those beans have little value.
}
