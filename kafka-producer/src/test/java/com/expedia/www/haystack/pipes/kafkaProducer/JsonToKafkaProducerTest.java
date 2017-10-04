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

import static com.expedia.www.haystack.pipes.kafkaProducer.JsonToKafkaProducer.CLIENT_ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JsonToKafkaProducerTest {
    @Mock
    private Metrics mockMetrics;
    private Metrics realMetrics;
    @Mock
    private JsonToKafkaProducer.Factory mockFactory;
    private JsonToKafkaProducer.Factory realFactory;

    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private KStream<String, String> mockKStream;
    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private ProduceIntoExternalKafkaAction mockProduceIntoExternalKafkaAction;

    private JsonToKafkaProducer jsonToKafkaProducer;

    @Before
    public void setUp() {
        realMetrics = JsonToKafkaProducer.metrics;
        JsonToKafkaProducer.metrics = mockMetrics;
        realFactory = JsonToKafkaProducer.factory;
        JsonToKafkaProducer.factory = mockFactory;
        jsonToKafkaProducer = new JsonToKafkaProducer(mockKafkaStreamStarter);
    }

    @After
    public void tearDown() {
        JsonToKafkaProducer.metrics = realMetrics;
        JsonToKafkaProducer.factory = realFactory;
        verifyNoMoreInteractions(mockMetrics, mockKStreamBuilder, mockKStream, mockKafkaStreamStarter,
                mockProduceIntoExternalKafkaAction);
    }

    @Test
    public void testDefaultConstructor() {
        jsonToKafkaProducer = new JsonToKafkaProducer();

        assertEquals(CLIENT_ID, jsonToKafkaProducer.kafkaStreamStarter.clientId);
        assertEquals(JsonToKafkaProducer.class, jsonToKafkaProducer.kafkaStreamStarter.containingClass);
    }

    @Test
    public void testMain() {
        final JsonToKafkaProducer instanceLoadedByClassLoader = JsonToKafkaProducer.instance;
        JsonToKafkaProducer.instance = jsonToKafkaProducer;

        JsonToKafkaProducer.main(null);

        verify(mockMetrics).startMetricsPolling();
        verify(mockKafkaStreamStarter).createAndStartStream(jsonToKafkaProducer);
        JsonToKafkaProducer.instance = instanceLoadedByClassLoader;
    }


    @Test
    public void testBuildStreamTopology() {
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<String>>any(), anyString()))
                .thenReturn(mockKStream);
        when(mockFactory.createProduceIntoExternalKafkaAction()).thenReturn(mockProduceIntoExternalKafkaAction);

        jsonToKafkaProducer.buildStreamTopology(mockKStreamBuilder);

        verify(mockKStreamBuilder).stream(Matchers.<Serde<String>>any(), Matchers.<Serde<String>>any(), eq("json-spans"));
        verify(mockFactory).createProduceIntoExternalKafkaAction();
        verify(mockKStream).foreach(mockProduceIntoExternalKafkaAction);
    }

    @Test
    public void testRealFactoryCreateProduceIntoExternalKafkaAction() {
        realFactory.createProduceIntoExternalKafkaAction();
    }
}
