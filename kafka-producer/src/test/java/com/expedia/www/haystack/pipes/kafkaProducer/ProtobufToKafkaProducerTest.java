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
package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
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

import static com.expedia.www.haystack.pipes.kafkaProducer.Constants.APPLICATION;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProtobufToKafkaProducerTest {
    @Mock
    private ProtobufToKafkaProducer.Factory mockFactory;
    private ProtobufToKafkaProducer.Factory realFactory;

    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private KStream<String, Span> mockKStream;
    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private ProduceIntoExternalKafkaAction mockProduceIntoExternalKafkaAction;

    private ProtobufToKafkaProducer protobufToKafkaProducer;

    @Before
    public void setUp() {
        realFactory = ProtobufToKafkaProducer.factory;
        ProtobufToKafkaProducer.factory = mockFactory;
        protobufToKafkaProducer = new ProtobufToKafkaProducer(mockKafkaStreamStarter, new SpanSerdeFactory());
    }

    @After
    public void tearDown() {
        ProtobufToKafkaProducer.factory = realFactory;
        verifyNoMoreInteractions(mockKStreamBuilder, mockKStream, mockKafkaStreamStarter,
                mockProduceIntoExternalKafkaAction);
    }

    @Test
    public void testDefaultConstructor() {
        protobufToKafkaProducer = new ProtobufToKafkaProducer();

        assertEquals(APPLICATION, protobufToKafkaProducer.kafkaStreamStarter.clientId);
        assertEquals(ProtobufToKafkaProducer.class, protobufToKafkaProducer.kafkaStreamStarter.containingClass);
    }

    @Test
    public void testMain() {
        final ProtobufToKafkaProducer instanceLoadedByClassLoader = ProtobufToKafkaProducer.instance;
        ProtobufToKafkaProducer.instance = protobufToKafkaProducer;

        protobufToKafkaProducer.main();

        verify(mockKafkaStreamStarter).createAndStartStream(protobufToKafkaProducer);
        ProtobufToKafkaProducer.instance = instanceLoadedByClassLoader;
    }


    @Test
    public void testBuildStreamTopology() {
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
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
