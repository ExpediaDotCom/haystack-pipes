package com.expedia.www.haystack.pipes;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.metrics.GraphiteConfig;
import com.expedia.www.haystack.metrics.MetricPublishing;
import org.apache.commons.text.StrSubstitutor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.cfg4j.provider.ConfigurationProvider;
import org.cfg4j.provider.ConfigurationProviderBuilder;
import org.cfg4j.source.classpath.ClasspathConfigurationSource;
import org.cfg4j.source.context.filesprovider.ConfigFilesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;

import static com.expedia.www.haystack.pipes.Constants.KAFKA_FROM_TOPIC;
import static com.expedia.www.haystack.pipes.Constants.KAFKA_TO_TOPIC;

public class ProtobufToJsonTransformer {
    static final String CLIENT_ID = "External";
    static final String STARTED_MSG = "Now started ScanStream";

    static Factory factory = new Factory(); // will be mocked out in unit tests
    static Logger logger = LoggerFactory.getLogger(ProtobufToJsonTransformer.class);

    // TODO Add EnvironmentVariablesConfigurationSource object to handle env variables from apply-compose.sh et al
    private static ConfigFilesProvider cfp = () -> Collections.singletonList(Paths.get("base.yaml"));
    private static ClasspathConfigurationSource ccs = new ClasspathConfigurationSource(cfp);
    private static ConfigurationProvider cp = new ConfigurationProviderBuilder().withConfigurationSource(ccs).build();

    static final String KLASS_NAME = ProtobufToJsonTransformer.class.getName();
    static final String KLASS_SIMPLE_NAME = ProtobufToJsonTransformer.class.getSimpleName();

    private static final StreamsConfig STREAMS_CONFIG = new StreamsConfig(getProperties());

    public static void main(String[] args) {
        startMetricsPolling();
        createAndStartStream();
    }

    private static void startMetricsPolling() {
        final GraphiteConfig graphiteConfig = cp.bind("haystack.graphite", GraphiteConfig.class);
        (new MetricPublishing()).start(graphiteConfig);
    }

    private static void createAndStartStream() {
        final KStreamBuilder kStreamBuilder = factory.createKStreamBuilder();
        buildStreamTopology(kStreamBuilder);
        startKafkaStreams(kStreamBuilder);
    }

    private static void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final Serde<Span> spanSerde = getSpanSerde();
        final Serde<String> stringSerde = Serdes.String();
        final KStream<String, Span> stream = kStreamBuilder.stream(stringSerde, spanSerde, KAFKA_FROM_TOPIC);
        stream.mapValues(span -> Span.newBuilder(span).build()).to(stringSerde, spanSerde, KAFKA_TO_TOPIC);
    }

    private static void startKafkaStreams(KStreamBuilder kStreamBuilder) {
        final KafkaStreams kafkaStreams = factory.createKafkaStreams(kStreamBuilder, STREAMS_CONFIG);
        final SystemExitUncaughtExceptionHandler closeThenRestartUncaughtExceptionHandler
                = factory.createSystemExitUncaughtExceptionHandler();
        kafkaStreams.setUncaughtExceptionHandler(closeThenRestartUncaughtExceptionHandler);
        kafkaStreams.start();
        logger.info(STARTED_MSG);
    }

    private static Serde<Span> getSpanSerde() {
        final SpanProtobufDeserializer protobufDeserializer = new SpanProtobufDeserializer();
        final SpanJsonSerializer spanJsonSerializer = new SpanJsonSerializer();
        return Serdes.serdeFrom(spanJsonSerializer, protobufDeserializer);
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

    private static String getKafkaIpAnPort() {
        final KafkaConfig kafkaConfig = cp.bind("haystack.kafka", KafkaConfig.class);
        return StrSubstitutor.replaceSystemProperties(kafkaConfig.brokers()) + ":" + kafkaConfig.port();
    }

    private static int getReplicationFactor() {
        final IntermediateStreamsConfig intermediateStreamsConfig = cp.bind("haystack.pipe.streams",
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
