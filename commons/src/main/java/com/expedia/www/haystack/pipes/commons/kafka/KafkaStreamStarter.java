package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.www.haystack.pipes.commons.Configuration;
import com.expedia.www.haystack.pipes.commons.IntermediateStreamsConfig;
import com.expedia.www.haystack.pipes.commons.SystemExitUncaughtExceptionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.cfg4j.provider.ConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStreamStarter {
    static Factory factory = new Factory(); // will be mocked out in unit tests
    static Logger logger = LoggerFactory.getLogger(KafkaStreamStarter.class);
    static final String STARTING_MSG = "Attempting to start stream pointing at Kafka [%s]";
    static final String STARTED_MSG = "Now started Stream %s";
    private static final ConfigurationProvider CONFIGURATION_PROVIDER =
            new Configuration().createMergeConfigurationProvider();

    public final Class<? extends KafkaStreamBuilder> containingClass;
    public final String clientId;
    final StreamsConfig streamsConfig;

    public KafkaStreamStarter(Class<? extends KafkaStreamBuilder> containingClass, String clientId) {
        this.containingClass = containingClass;
        this.clientId = clientId;
        this.streamsConfig = new StreamsConfig(getProperties());
    }

    public void createAndStartStream(KafkaStreamBuilder kafkaStreamBuilder) {
        final KStreamBuilder kStreamBuilder = factory.createKStreamBuilder();
        kafkaStreamBuilder.buildStreamTopology(kStreamBuilder);
        startKafkaStreams(kStreamBuilder);
    }

    private void startKafkaStreams(KStreamBuilder kStreamBuilder) {
        final KafkaStreams kafkaStreams = factory.createKafkaStreams(kStreamBuilder, this);
        final SystemExitUncaughtExceptionHandler systemExitUncaughtExceptionHandler
                = factory.createSystemExitUncaughtExceptionHandler(kafkaStreams);
        kafkaStreams.setUncaughtExceptionHandler(systemExitUncaughtExceptionHandler);
        logger.info(String.format(STARTING_MSG, getKafkaIpAnPort()));
        kafkaStreams.start();
        logger.info(String.format(STARTED_MSG, kStreamBuilder.getClass().getSimpleName()));
    }

    Properties getProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, containingClass.getName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, containingClass.getSimpleName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaIpAnPort());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, getReplicationFactor());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

    private String getKafkaIpAnPort() {
        final KafkaConfig kafkaConfig = getKafkaConfig();
        return kafkaConfig.brokers() + ":" + kafkaConfig.port();
    }

    private int getReplicationFactor() {
        final IntermediateStreamsConfig intermediateStreamsConfig = CONFIGURATION_PROVIDER.bind(
                Configuration.HAYSTACK_PIPE_STREAMS, IntermediateStreamsConfig.class);
        return intermediateStreamsConfig.replicationfactor();
    }

    private static KafkaConfig getKafkaConfig() {
        return CONFIGURATION_PROVIDER.bind(Configuration.HAYSTACK_KAFKA_CONFIG_PREFIX, KafkaConfig.class);
    }

    static class Factory {
        KStreamBuilder createKStreamBuilder() {
            return new KStreamBuilder();
        }

        KafkaStreams createKafkaStreams(KStreamBuilder kStreamBuilder, KafkaStreamStarter kafkaStreamStarter) {
            return new KafkaStreams(kStreamBuilder, kafkaStreamStarter.streamsConfig);
        }

        SystemExitUncaughtExceptionHandler createSystemExitUncaughtExceptionHandler(KafkaStreams kafkaStreams) {
            return new SystemExitUncaughtExceptionHandler(kafkaStreams);
        }

    }
}
