/*
 * Copyright 2018 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.www.haystack.commons.config.Configuration;
import com.expedia.www.haystack.pipes.commons.IntermediateStreamsConfig;
import com.expedia.www.haystack.pipes.commons.SystemExitUncaughtExceptionHandler;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.cfg4j.provider.ConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_KAFKA_CONFIG_PREFIX;
import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_PIPE_STREAMS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KafkaStreamStarter {
    // move this to configuration later
    private final long MAX_CLOSE_TIMEOUT_SEC = 30;

    @VisibleForTesting
    static Factory factory = new Factory();
    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(KafkaStreamStarter.class);
    @VisibleForTesting
    static ConfigurationProvider CONFIGURATION_PROVIDER = new Configuration().createMergeConfigurationProvider();

    static final String STARTING_MSG_WITH_TO_TOPIC =
            "Attempting to start stream pointing at Kafka [%s] from topic [%s] to topic [%s]";
    static final String STARTING_MSG_WITHOUT_TO_TOPIC =
            "Attempting to start stream pointing at Kafka [%s] from topic [%s]";
    static final String STARTED_MSG = "Now started Stream %s";

    private final HealthController healthController;

    public final Class<? extends KafkaStreamBuilder> containingClass;
    public final String clientId;
    private final StreamsConfig streamsConfig;

    public KafkaStreamStarter(Class<? extends KafkaStreamBuilder> containingClass,
                              String clientId,
                              HealthController healthController) {
        this.containingClass = containingClass;
        this.clientId = clientId;
        this.healthController = healthController;
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
                = factory.createSystemExitUncaughtExceptionHandler(kafkaStreams, healthController);
        kafkaStreams.setUncaughtExceptionHandler(systemExitUncaughtExceptionHandler);
        final String toTopic = getToTopic();
        if(StringUtils.isEmpty(toTopic)) {
            logger.info(String.format(STARTING_MSG_WITHOUT_TO_TOPIC, getIpAnPort(), getFromTopic()));
        } else {
            logger.info(String.format(STARTING_MSG_WITH_TO_TOPIC, getIpAnPort(), getFromTopic(), toTopic));
        }
        kafkaStreams.start();
        healthController.setHealthy();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaStreams.close(MAX_CLOSE_TIMEOUT_SEC, SECONDS)));
        logger.info(String.format(STARTED_MSG, kStreamBuilder.getClass().getSimpleName()));
    }

    Properties getProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, containingClass.getName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, containingClass.getSimpleName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getIpAnPort());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, getReplicationFactor());
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), getConsumerSessionTimeout());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, getThreadCount());
        return props;
    }

    private String getIpAnPort() {
        final KafkaConfig kafkaConfig = getKafkaConfig();
        return kafkaConfig.brokers() + ":" + kafkaConfig.port();
    }

    private String getFromTopic() {
        final KafkaConfig kafkaConfig = getKafkaConfig();
        return kafkaConfig.fromtopic();
    }

    private String getToTopic() {
        final KafkaConfig kafkaConfig = getKafkaConfig();
        return kafkaConfig.totopic();
    }

    private int getThreadCount() {
        final KafkaConfig kafkaConfig = getKafkaConfig();
        return kafkaConfig.threadcount();
    }

    private int getReplicationFactor() {
        final IntermediateStreamsConfig intermediateStreamsConfig = CONFIGURATION_PROVIDER.bind(
                HAYSTACK_PIPE_STREAMS, IntermediateStreamsConfig.class);
        return intermediateStreamsConfig.replicationfactor();
    }

    private int getConsumerSessionTimeout() {
        final KafkaConfig kafkaConfig = getKafkaConfig();
        return kafkaConfig.sessiontimeout();
    }

    private static KafkaConfig getKafkaConfig() {
        return CONFIGURATION_PROVIDER.bind(HAYSTACK_KAFKA_CONFIG_PREFIX, KafkaConfig.class);
    }

    static class Factory {
        KStreamBuilder createKStreamBuilder() {
            return new KStreamBuilder();
        }

        KafkaStreams createKafkaStreams(KStreamBuilder kStreamBuilder, KafkaStreamStarter kafkaStreamStarter) {
            return new KafkaStreams(kStreamBuilder, kafkaStreamStarter.streamsConfig);
        }

        SystemExitUncaughtExceptionHandler createSystemExitUncaughtExceptionHandler(KafkaStreams kafkaStreams,
                                                                                    HealthController controller) {
            return new SystemExitUncaughtExceptionHandler(kafkaStreams, controller);
        }
    }
}
