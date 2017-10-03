package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.www.haystack.pipes.commons.Configuration;
import com.expedia.www.haystack.pipes.commons.IntermediateStreamsConfig;
import com.expedia.www.haystack.pipes.commons.KafkaConfig;
import com.expedia.www.haystack.pipes.commons.SystemExitUncaughtExceptionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.cfg4j.provider.ConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class JsonToKafkaProducer {
    static final String CLIENT_ID = "haystack-pipes-json-to-kafka-producer";
    static final String STARTED_MSG = "Now started Producer stream to send JSON spans to external Kafka";
    private static final Configuration CONFIGURATION = new Configuration();
    private static final ConfigurationProvider CONFIGURATION_PROVIDER = CONFIGURATION.createMergeConfigurationProvider();
    private static final StreamsConfig STREAMS_CONFIG = new StreamsConfig(getProperties());
    static Factory factory = new Factory(); // will be mocked out in unit tests
    static Logger logger = LoggerFactory.getLogger(JsonToKafkaProducer.class);
    static final String KLASS_NAME = JsonToKafkaProducer.class.getName();
    static final String KLASS_SIMPLE_NAME = JsonToKafkaProducer.class.getSimpleName();

    public static void main(String[] args) {
        createAndStartStream();
    }

    private static void createAndStartStream() {
        final KStreamBuilder kStreamBuilder = factory.createKStreamBuilder();
        buildStreamTopology(kStreamBuilder);
        startKafkaStreams(kStreamBuilder);
    }

    private static void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final Serde<String> stringSerde = Serdes.String();
        final KStream<String, String> stream = kStreamBuilder.stream(stringSerde, stringSerde, getFromTopic());
        final ForeachAction<String, String> produceIntoExternalKafkaAction = new ProduceIntoExternalKafkaAction();
        stream.foreach(produceIntoExternalKafkaAction);
    }

    private static String getKafkaIpAnPort() {
        final KafkaConfig kafkaConfig = getKafkaConfig();
        return kafkaConfig.brokers() + ":" + kafkaConfig.port();
    }

    private static void startKafkaStreams(KStreamBuilder kStreamBuilder) {
        final KafkaStreams kafkaStreams = factory.createKafkaStreams(kStreamBuilder, STREAMS_CONFIG);
        final SystemExitUncaughtExceptionHandler closeThenRestartUncaughtExceptionHandler
                = factory.createSystemExitUncaughtExceptionHandler();
        kafkaStreams.setUncaughtExceptionHandler(closeThenRestartUncaughtExceptionHandler);
        kafkaStreams.start();
        logger.info(STARTED_MSG);
    }

    static Properties getProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KLASS_NAME);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KLASS_SIMPLE_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaIpAnPort());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, getReplicationFactor());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }


    static String getFromTopic() {
        return getKafkaConfig().fromTopic();
    }

    private static KafkaConfig getKafkaConfig() {
        return CONFIGURATION_PROVIDER.bind(Configuration.HAYSTACK_KAFKA_CONFIG_PREFIX, KafkaConfig.class);
    }

    private static int getReplicationFactor() {
        final IntermediateStreamsConfig intermediateStreamsConfig = CONFIGURATION_PROVIDER.bind("haystack.pipe.streams",
                IntermediateStreamsConfig.class);
        return intermediateStreamsConfig.replicationFactor();
    }

    static class Factory {
        KStreamBuilder createKStreamBuilder() {
            return new KStreamBuilder();
        }

        KafkaStreams createKafkaStreams(KStreamBuilder kStreamBuilder, StreamsConfig streamsConfig) {
            return new KafkaStreams(kStreamBuilder, streamsConfig);
        }

        SystemExitUncaughtExceptionHandler createSystemExitUncaughtExceptionHandler() {
            return new SystemExitUncaughtExceptionHandler();
        }

    }
}
