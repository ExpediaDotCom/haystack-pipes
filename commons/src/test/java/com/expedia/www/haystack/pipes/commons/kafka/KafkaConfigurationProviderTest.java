package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KafkaConfigurationProviderTest {
    private KafkaConfigurationProvider kafkaConfigurationProvider;

    @Before
    public void setUp() {
        kafkaConfigurationProvider = new KafkaConfigurationProvider();
    }

    @Test
    public void testBrokers() {
        assertEquals("localhost", kafkaConfigurationProvider.brokers());
    }
    
    @Test
    public void testPort() {
        assertEquals(65534, kafkaConfigurationProvider.port());
    }
    
    @Test
    public void testFromTopic() {
        assertEquals("haystack.kafka.fromtopic", kafkaConfigurationProvider.fromtopic());
    }
    
    @Test
    public void testToTopic() {
        assertEquals("haystack.kafka.totopic", kafkaConfigurationProvider.totopic());
    }
}
