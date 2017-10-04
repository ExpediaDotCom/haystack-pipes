package com.expedia.www.haystack.pipes.commons;

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
        assertEquals("haystack.kafka.fromTopic", kafkaConfigurationProvider.fromTopic());
    }
    
    @Test
    public void testToTopic() {
        assertEquals("haystack.kafka.toTopic", kafkaConfigurationProvider.toTopic());
    }
}
