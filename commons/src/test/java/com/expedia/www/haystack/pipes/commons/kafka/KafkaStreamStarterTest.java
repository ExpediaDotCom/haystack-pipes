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
package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.www.haystack.pipes.commons.SystemExitUncaughtExceptionHandler;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter.Factory;
import com.netflix.servo.publish.PollScheduler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Properties;
import java.util.regex.Pattern;

import static com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter.STARTED_MSG;
import static com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter.STARTING_MSG;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStreamStarterTest {
    private final static String CLIENT_ID = RANDOM.nextLong() + "CLIENT_ID";
    private static final String KAFKA_IP_AND_PORT = "localhost:" + 65534;
    private static final String KAFKA_FROM_TOPIC = "haystack.kafka.fromtopic";
    private static final String KAFKA_TO_TOPIC = "haystack.kafka.totopic";

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

    private KafkaStreamStarter kafkaStreamStarter;

    @Before
    public void setUp() {
        realFactory = KafkaStreamStarter.factory;
        KafkaStreamStarter.factory = mockFactory;
        realLogger = KafkaStreamStarter.logger;
        KafkaStreamStarter.logger = mockLogger;
        kafkaStreamStarter = new KafkaStreamStarter(mockKafkaStreamBuilder.getClass(), CLIENT_ID);
    }

    @After
    public void tearDown() {
        KafkaStreamStarter.factory = realFactory;
        KafkaStreamStarter.logger = realLogger;
        if (PollScheduler.getInstance().isStarted()) {
            PollScheduler.getInstance().stop();
        }
        verifyNoMoreInteractions(mockFactory, mockLogger, mockKafkaStreamBuilder, mockKStreamBuilder,
                mockKafkaStreams, mockSystemExitUncaughtExceptionHandler);
    }

    @Test
    public void testCreateAndStartStream() {
        when(mockFactory.createKStreamBuilder()).thenReturn(mockKStreamBuilder);
        when(mockFactory.createKafkaStreams(mockKStreamBuilder, kafkaStreamStarter))
                .thenReturn(mockKafkaStreams);
        when(mockFactory.createSystemExitUncaughtExceptionHandler(mockKafkaStreams))
                .thenReturn(mockSystemExitUncaughtExceptionHandler);

        kafkaStreamStarter.createAndStartStream(mockKafkaStreamBuilder);

        verify(mockFactory).createKStreamBuilder();
        verify(mockFactory).createKafkaStreams(mockKStreamBuilder, kafkaStreamStarter);
        verify(mockKafkaStreamBuilder).buildStreamTopology(mockKStreamBuilder);
        verify(mockFactory).createSystemExitUncaughtExceptionHandler(mockKafkaStreams);
        verify(mockKafkaStreams).setUncaughtExceptionHandler(mockSystemExitUncaughtExceptionHandler);
        verify(mockKafkaStreams).start();
        verify(mockLogger).info(String.format(STARTING_MSG, KAFKA_IP_AND_PORT, KAFKA_FROM_TOPIC, KAFKA_TO_TOPIC));
        verify(mockLogger).info(String.format(STARTED_MSG, mockKStreamBuilder.getClass().getSimpleName()));

    }

    @Test
    public void testGetProperties() {
        final Properties properties = kafkaStreamStarter.getProperties();
        assertEquals(6, properties.size());
        assertEquals(CLIENT_ID, properties.get(StreamsConfig.CLIENT_ID_CONFIG));
        assertEquals(mockKafkaStreamBuilder.getClass().getName(), properties.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(mockKafkaStreamBuilder.getClass().getSimpleName(), properties.get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals(KAFKA_IP_AND_PORT, properties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(2147483645, properties.get(StreamsConfig.REPLICATION_FACTOR_CONFIG));
        assertEquals(WallclockTimestampExtractor.class, properties.get(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG));
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

        verify(mockKStreamBuilder).latestResetTopicsPattern();
        verify(mockKStreamBuilder).earliestResetTopicsPattern();
        verify(mockKStreamBuilder, times(2)).globalStateStores();
        verify(mockKStreamBuilder).buildGlobalStateTopology();
        verify(mockKStreamBuilder).sourceTopicPattern();
    }

    @Test
    public void testFactoryCreateSystemExitUncaughtExceptionHandler() {
        assertNotNull(realFactory.createSystemExitUncaughtExceptionHandler(mockKafkaStreams));
    }

}
