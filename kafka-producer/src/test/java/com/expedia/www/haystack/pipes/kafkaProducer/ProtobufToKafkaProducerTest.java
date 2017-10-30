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
package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.www.haystack.pipes.commons.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.kafkaProducer.ProtobufToKafkaProducer.CLIENT_ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProtobufToKafkaProducerTest {
    @Mock
    private Metrics mockMetrics;
    private Metrics realMetrics;
    @Mock
    private ProtobufToKafkaProducer.Factory mockFactory;
    private ProtobufToKafkaProducer.Factory realFactory;

    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private KStream<String, String> mockKStream;
    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private ProduceIntoExternalKafkaAction mockProduceIntoExternalKafkaAction;

    private ProtobufToKafkaProducer protobufToKafkaProducer;

    @Before
    public void setUp() {
        realMetrics = ProtobufToKafkaProducer.metrics;
        ProtobufToKafkaProducer.metrics = mockMetrics;
        realFactory = ProtobufToKafkaProducer.factory;
        ProtobufToKafkaProducer.factory = mockFactory;
        protobufToKafkaProducer = new ProtobufToKafkaProducer(mockKafkaStreamStarter);
    }

    @After
    public void tearDown() {
        ProtobufToKafkaProducer.metrics = realMetrics;
        ProtobufToKafkaProducer.factory = realFactory;
        verifyNoMoreInteractions(mockMetrics, mockKStreamBuilder, mockKStream, mockKafkaStreamStarter,
                mockProduceIntoExternalKafkaAction);
    }

    @Test
    public void testDefaultConstructor() {
        protobufToKafkaProducer = new ProtobufToKafkaProducer();

        assertEquals(CLIENT_ID, protobufToKafkaProducer.kafkaStreamStarter.clientId);
        assertEquals(ProtobufToKafkaProducer.class, protobufToKafkaProducer.kafkaStreamStarter.containingClass);
    }

    @Test
    public void testMain() {
        final ProtobufToKafkaProducer instanceLoadedByClassLoader = ProtobufToKafkaProducer.instance;
        ProtobufToKafkaProducer.instance = protobufToKafkaProducer;

        ProtobufToKafkaProducer.main(null);

        verify(mockMetrics).startMetricsPolling();
        verify(mockKafkaStreamStarter).createAndStartStream(protobufToKafkaProducer);
        ProtobufToKafkaProducer.instance = instanceLoadedByClassLoader;
    }


    @Test
    public void testBuildStreamTopology() {
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<String>>any(), anyString()))
                .thenReturn(mockKStream);
        when(mockFactory.createProduceIntoExternalKafkaAction()).thenReturn(mockProduceIntoExternalKafkaAction);

        protobufToKafkaProducer.buildStreamTopology(mockKStreamBuilder);

        verify(mockKStreamBuilder).stream(Matchers.<Serde<String>>any(), Matchers.<Serde<String>>any(), eq("json-spans"));
        verify(mockFactory).createProduceIntoExternalKafkaAction();
        verify(mockKStream).foreach(mockProduceIntoExternalKafkaAction);
    }

    @Test
    public void testRealFactoryCreateProduceIntoExternalKafkaAction() {
        realFactory.createProduceIntoExternalKafkaAction();
    }
}
