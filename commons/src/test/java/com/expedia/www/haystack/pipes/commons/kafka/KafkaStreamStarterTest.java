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

import com.expedia.www.haystack.pipes.commons.Configuration;
import com.expedia.www.haystack.pipes.commons.SystemExitUncaughtExceptionHandler;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter.Factory;
import com.netflix.servo.publish.PollScheduler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.cfg4j.provider.ConfigurationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Properties;
import java.util.regex.Pattern;

import static com.expedia.www.haystack.pipes.commons.Configuration.HAYSTACK_KAFKA_CONFIG_PREFIX;
import static com.expedia.www.haystack.pipes.commons.ConfigurationTest.THREAD_COUNT_CONFIGURATION_IN_TEST_BASE_DOT_YAML;
import static com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter.STARTED_MSG;
import static com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter.STARTING_MSG_WITHOUT_TO_TOPIC;
import static com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter.STARTING_MSG_WITH_TO_TOPIC;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStreamStarterTest {
    private static final String CLIENT_ID = RANDOM.nextLong() + "CLIENT_ID";
    private static final String BROKERS = "localhost";
    private static final int PORT = 65534;
    private static final String KAFKA_IP_AND_PORT = BROKERS + ":" + PORT;
    private static final String KAFKA_FROM_TOPIC = "haystack.kafka.fromtopic";
    private static final String KAFKA_TO_TOPIC = "haystack.kafka.totopic";

    private final HealthController healthController = new HealthController();

    @Mock
    private Factory mockFactory;
    private Factory realFactory;
    @Mock
    private Logger mockLogger;
    private Logger realLogger;
    @Mock
    private KafkaStreamBuilder mockKafkaStreamBuilder;
    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private KafkaStreams mockKafkaStreams;
    @Mock
    private SystemExitUncaughtExceptionHandler mockSystemExitUncaughtExceptionHandler;
    @Mock
    private KafkaConfig mockKafkaConfig;

    private KafkaStreamStarter kafkaStreamStarter;


    @Before
    public void setUp() {
        realFactory = KafkaStreamStarter.factory;
        KafkaStreamStarter.factory = mockFactory;
        realLogger = KafkaStreamStarter.logger;
        KafkaStreamStarter.logger = mockLogger;
        KafkaConfigurationProvider configurationProvider = new KafkaConfigurationProvider();
        kafkaStreamStarter = new KafkaStreamStarter(mockKafkaStreamBuilder.getClass(), CLIENT_ID,configurationProvider,healthController);
    }

    @After
    public void tearDown() {
        KafkaStreamStarter.factory = realFactory;
        KafkaStreamStarter.logger = realLogger;
        if (PollScheduler.getInstance().isStarted()) {
            PollScheduler.getInstance().stop();
        }
        verifyNoMoreInteractions(mockFactory, mockLogger, mockKafkaStreamBuilder, mockKStreamBuilder,
                mockKafkaStreams, mockSystemExitUncaughtExceptionHandler, mockKafkaConfig);
    }

    @Test
    public void testCreateAndStartStreamWithToTopic() {
        commonWhensForCreateAndStartStream();

        kafkaStreamStarter.createAndStartStream(mockKafkaStreamBuilder);

        commonVerifiesForCreateAndStartStream();
        verify(mockLogger).info(String.format(STARTING_MSG_WITH_TO_TOPIC, KAFKA_IP_AND_PORT, KAFKA_FROM_TOPIC, KAFKA_TO_TOPIC));
    }

    @Test
    public void testCreateAndStartStreamWithoutToTopic() {
        commonWhensForCreateAndStartStream();
        when(mockKafkaConfig.fromtopic()).thenReturn(KAFKA_FROM_TOPIC);
        when(mockKafkaConfig.brokers()).thenReturn(BROKERS);
        when(mockKafkaConfig.port()).thenReturn(PORT);

        final KafkaConfig savedKafkaConfig = KafkaStreamStarter.kafkaConfig;
        KafkaStreamStarter.kafkaConfig = mockKafkaConfig;
        kafkaStreamStarter.createAndStartStream(mockKafkaStreamBuilder);
        kafkaStreamStarter.kafkaConfig = savedKafkaConfig;

        commonVerifiesForCreateAndStartStream();
        verify(mockLogger).info(String.format(STARTING_MSG_WITHOUT_TO_TOPIC, KAFKA_IP_AND_PORT, KAFKA_FROM_TOPIC));
        verify(mockKafkaConfig).fromtopic();
        verify(mockKafkaConfig).totopic();
        verify(mockKafkaConfig).brokers();
        verify(mockKafkaConfig).port();
    }

    private void commonWhensForCreateAndStartStream() {
        when(mockFactory.createKStreamBuilder()).thenReturn(mockKStreamBuilder);
        when(mockFactory.createKafkaStreams(mockKStreamBuilder, kafkaStreamStarter))
                .thenReturn(mockKafkaStreams);
        when(mockFactory.createSystemExitUncaughtExceptionHandler(mockKafkaStreams, healthController))
                .thenReturn(mockSystemExitUncaughtExceptionHandler);
    }

    private void commonVerifiesForCreateAndStartStream() {
        verify(mockFactory).createKStreamBuilder();
        verify(mockFactory).createKafkaStreams(mockKStreamBuilder, kafkaStreamStarter);
        verify(mockKafkaStreamBuilder).buildStreamTopology(mockKStreamBuilder);
        verify(mockFactory).createSystemExitUncaughtExceptionHandler(mockKafkaStreams, healthController);
        verify(mockKafkaStreams).setUncaughtExceptionHandler(mockSystemExitUncaughtExceptionHandler);
        verify(mockKafkaStreams).start();
        verify(mockLogger).info(String.format(STARTED_MSG, mockKStreamBuilder.getClass().getSimpleName()));
    }

    @Test
    public void testGetProperties() {
        final Properties properties = kafkaStreamStarter.getProperties();
        assertEquals(CLIENT_ID, properties.remove(StreamsConfig.CLIENT_ID_CONFIG));
        assertEquals(mockKafkaStreamBuilder.getClass().getName(), properties.remove(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(mockKafkaStreamBuilder.getClass().getSimpleName(), properties.remove(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals(KAFKA_IP_AND_PORT, properties.remove(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(THREAD_COUNT_CONFIGURATION_IN_TEST_BASE_DOT_YAML, properties.remove(StreamsConfig.NUM_STREAM_THREADS_CONFIG));
        assertEquals(15000, properties.remove(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG)));
        assertEquals("Properties should be empty but is: " + properties, 0, properties.size());
    }


    @Test
    public void testFactoryCreateKStreamBuilder() {
        assertNotNull(realFactory.createKStreamBuilder());
    }

    @Test
    public void testFactoryCreateKafkaStreams() {
        final Pattern emptyStringPattern = Pattern.compile("");
        when(mockKStreamBuilder.latestResetTopicsPattern()).thenReturn(emptyStringPattern);
        when(mockKStreamBuilder.earliestResetTopicsPattern()).thenReturn(emptyStringPattern);

        realFactory.createKafkaStreams(mockKStreamBuilder, kafkaStreamStarter);

        verify(mockKStreamBuilder, times(THREAD_COUNT_CONFIGURATION_IN_TEST_BASE_DOT_YAML)).latestResetTopicsPattern();
        verify(mockKStreamBuilder, times(THREAD_COUNT_CONFIGURATION_IN_TEST_BASE_DOT_YAML)).earliestResetTopicsPattern();
        verify(mockKStreamBuilder, times(2)).globalStateStores();
        verify(mockKStreamBuilder).buildGlobalStateTopology();
        verify(mockKStreamBuilder, times(THREAD_COUNT_CONFIGURATION_IN_TEST_BASE_DOT_YAML)).sourceTopicPattern();
    }

    @Test
    public void testFactoryCreateSystemExitUncaughtExceptionHandler() {
        assertNotNull(realFactory.createSystemExitUncaughtExceptionHandler(mockKafkaStreams, healthController));
    }

}
