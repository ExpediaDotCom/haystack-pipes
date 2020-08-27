package com.expedia.www.haystack.pipes.commons.kafka.config;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProjectConfigurationTest {

    private ProjectConfiguration projectConfiguration;

    @Before
    public void setUp() {
        projectConfiguration = ProjectConfiguration.getInstance();
    }

    @Test
    public void getInstance() {
        ProjectConfiguration anotherProjectConfiguration = ProjectConfiguration.getInstance();
        assertEquals(anotherProjectConfiguration, projectConfiguration);
    }

    @Test
    public void testGetKafkaConsumerConfig() {
        KafkaConsumerConfig kafkaConsumerConfig = projectConfiguration.getKafkaConsumerConfig();
        assertEquals(kafkaConsumerConfig.getBrokers(), "localhost");
        assertEquals(kafkaConsumerConfig.getPort(), 65534);
        assertEquals(kafkaConsumerConfig.getFromTopic(), "haystack.kafka.fromtopic");
        assertEquals(kafkaConsumerConfig.getToTopic(), "haystack.kafka.totopic");
        assertEquals(kafkaConsumerConfig.getThreadCount(), 42);
        assertEquals(kafkaConsumerConfig.getSessionTimeout(), 15000);
        assertEquals(kafkaConsumerConfig.getMaxWakeUps(), 10);
        assertEquals(kafkaConsumerConfig.getWakeUpTimeoutMs(), 3000);
        assertEquals(kafkaConsumerConfig.getPollTimeoutMs(), 250);
        assertEquals(kafkaConsumerConfig.getCommitMs(), 3000);
    }

    @Test
    public void testGetPipesConfig() {
        PipesConfig pipesConfig = projectConfiguration.getPipesConfig();
        assertEquals(pipesConfig.getReplicationFactor(), 2147483645);
    }

    @Test
    public void testGetKafkaProducerConfig() {
        KafkaProducerConfig kafkaProducerConfig = projectConfiguration.getKafkaProducerConfig();
        assertEquals(kafkaProducerConfig.getBrokers(), "localhost:9092");
        assertEquals(kafkaProducerConfig.getPort(), 9092);
        assertEquals(kafkaProducerConfig.getAcks(), "0");
        assertEquals(kafkaProducerConfig.getBatchSize(), 16384);
        assertEquals(kafkaProducerConfig.getLingerMs(), 5);
        assertEquals(kafkaProducerConfig.getBufferMemory(), 1048576);
        assertEquals(kafkaProducerConfig.getSpanKeyExtractorConfig().getExtractorConfig().size(), 1);
    }

    @Test
    public void getFirehoseConfig() {
        FirehoseConfig firehoseConfig = projectConfiguration.getFirehoseConfig();
        assertEquals(firehoseConfig.getInitialRetrySleep(), 42);
        assertEquals(firehoseConfig.getMaxRetrySleep(), 5000);
        assertEquals(firehoseConfig.getUrl(), "https://firehose.us-west-2.amazonaws.com");
        assertEquals(firehoseConfig.getStreamName(), "haystack-traces-test");
        assertEquals(firehoseConfig.getSigningRegion(), "us-west-2");
        assertEquals(firehoseConfig.getMaxBatchInterval(), 0);
        assertEquals(firehoseConfig.getMaxParallelISMPerShard(), 10);
        assertEquals(firehoseConfig.isUseStringBuffering(), true);
    }

    @Test
    public void getHttpPostConfig() {
        HttpPostConfig httpPostConfig = projectConfiguration.getHttpPostConfig();
        assertEquals(httpPostConfig.getUrl(), "http://localhost");
        assertEquals(httpPostConfig.getMaxBytes(), "1572864");
        assertEquals(httpPostConfig.getSeparator(), "\n");
        assertEquals(httpPostConfig.getHeaders().size(), 1);
    }


}
