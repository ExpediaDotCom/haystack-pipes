/*
 * Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.jsonTransformer;

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
import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.classpath.ClasspathConfigurationSource;
import org.cfg4j.source.compose.MergeConfigurationSource;
import org.cfg4j.source.system.EnvironmentVariablesConfigurationSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;

public class ProtobufToJsonTransformer {
    static final String CLIENT_ID = "haystack-pipes-protobuf-to-json-transformer";
    static final String STARTED_MSG = "Now started ScanStream";
    private static final String HAYSTACK_GRAPHITE_CONFIG_PREFIX = "haystack.graphite";
    private static final ConfigurationProvider CONFIGURATION_PROVIDER = createMergeConfigurationProvider();

    static Factory factory = new Factory(); // will be mocked out in unit tests
    static Logger logger = LoggerFactory.getLogger(ProtobufToJsonTransformer.class);

    private static ConfigurationProvider createMergeConfigurationProvider() {
        final MergeConfigurationSource configurationSource = new MergeConfigurationSource(
                createClasspathConfigurationSource(), createEnvironmentConfigurationSource()
        );
        final ConfigurationProviderBuilder configurationProviderBuilder = new ConfigurationProviderBuilder();
        return configurationProviderBuilder.withConfigurationSource(configurationSource).build();
    }

    private static ConfigurationSource createClasspathConfigurationSource() {
        return new ClasspathConfigurationSource(() -> Collections.singletonList(Paths.get("base.yaml")));
    }

    private static ConfigurationSource createEnvironmentConfigurationSource() {
        final EnvironmentVariablesConfigurationSource environmentVariablesConfigurationSource =
                new EnvironmentVariablesConfigurationSource();
        return new ChangeEnvVarsToLowerCaseConfigurationSource("HAYSTACK", environmentVariablesConfigurationSource);
    }

    static final String KLASS_NAME = ProtobufToJsonTransformer.class.getName();
    static final String KLASS_SIMPLE_NAME = ProtobufToJsonTransformer.class.getSimpleName();

    private static final StreamsConfig STREAMS_CONFIG = new StreamsConfig(getProperties());

    /**
     * main() is an instance method because it is called by the static void IsActiveController.main(String [] args);
     * making it an instance method facilitates unit testing.
     */
    void main() {
        startMetricsPolling();
        createAndStartStream();
    }

    private static void startMetricsPolling() {
        final GraphiteConfig graphiteConfig = CONFIGURATION_PROVIDER.bind(
                HAYSTACK_GRAPHITE_CONFIG_PREFIX, GraphiteConfig.class);
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
        final KStream<String, Span> stream = kStreamBuilder.stream(stringSerde, spanSerde, getFromTopic());
        stream.mapValues(span -> Span.newBuilder(span).build()).to(stringSerde, spanSerde, getToTopic());
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
        final KafkaConfig kafkaConfig = getKafkaConfig();
        return StrSubstitutor.replaceSystemProperties(kafkaConfig.brokers()) + ":" + kafkaConfig.port();
    }
    
    static String getFromTopic() {
        return getKafkaConfig().fromTopic();
    }

    static String getToTopic() {
        return getKafkaConfig().toTopic();
    }

    private static KafkaConfig getKafkaConfig() {
        return CONFIGURATION_PROVIDER.bind("haystack.kafka", KafkaConfig.class);
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
