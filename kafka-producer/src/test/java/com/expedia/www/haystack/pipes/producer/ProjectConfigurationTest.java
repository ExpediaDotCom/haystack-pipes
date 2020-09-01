package com.expedia.www.haystack.pipes.producer;

import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.producer.config.KafkaProducerConfig;
import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProjectConfigurationTest {

    private ProjectConfiguration projectConfiguration;

    @Before
    public void setUp() {
        projectConfiguration = ProjectConfiguration.getInstance();
    }

    @Test
    public void testGetInstance() {
        ProjectConfiguration newProjectConfiguration = ProjectConfiguration.getInstance();
        assertEquals(projectConfiguration, newProjectConfiguration);
    }

    @Test
    public void testGetKafkaConsumerConfig() {
        KafkaConsumerConfig kafkaConsumerConfig = projectConfiguration.getKafkaConsumerConfig();
        assertEquals("localhost", kafkaConsumerConfig.brokers());
        assertEquals(9092, kafkaConsumerConfig.port());
        assertEquals("json-spans", kafkaConsumerConfig.fromtopic());
        assertEquals(1, kafkaConsumerConfig.threadcount());
        assertEquals(15000, kafkaConsumerConfig.sessiontimeout());
        assertEquals(10, kafkaConsumerConfig.maxwakeups());
        assertEquals(3000, kafkaConsumerConfig.wakeuptimeoutms());
        assertEquals(250, kafkaConsumerConfig.polltimeoutms());
        assertEquals(3000, kafkaConsumerConfig.commitms());
    }

    @Test
    public void getKafkaProducerConfigList() {
        List<KafkaProducerConfig> kafkaProducerConfigs = projectConfiguration.getKafkaProducerConfigList();
        assertEquals(1, kafkaProducerConfigs.size());
    }

    @Test
    public void testGetSpanExtractorConfigs() {
        Map<String, Config> spanKeyExtractorConfigMap = projectConfiguration.getSpanExtractorConfigs();
        assertEquals(1, spanKeyExtractorConfigMap.size());
    }
}