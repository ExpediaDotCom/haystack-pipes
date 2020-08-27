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
package com.expedia.www.haystack.pipes.kafka.producer;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.kafka.producer.Constants.APPLICATION;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ProtobufToKafkaProducerTest {
    private static final String FROM_TOPIC = RANDOM.nextLong() + "FROM_TOPIC";

    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private SerdeFactory mockSerdeFactory;
    @Mock
    private ProduceIntoExternalKafkaAction mockProduceIntoExternalKafkaAction;
    @Mock
    private KafkaConsumerConfig mockKafkaConsumerConfig;
    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private KStream<String, Span> mockKStream;
    @Mock
    private Serde<Span> mockSpanSerde;

    private ProtobufToKafkaProducer protobufToFirehoseProducer;

    @Before
    public void setUp() {
        protobufToFirehoseProducer = new ProtobufToKafkaProducer(
                mockKafkaStreamStarter, mockSerdeFactory, mockProduceIntoExternalKafkaAction, mockKafkaConsumerConfig);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockKafkaStreamStarter, mockSerdeFactory, mockProduceIntoExternalKafkaAction,
                mockKafkaConsumerConfig, mockKStreamBuilder, mockKStream, mockSpanSerde);
    }

    @Test
    public void testMain() {
        protobufToFirehoseProducer.main();

        verify(mockKafkaStreamStarter).createAndStartStream(protobufToFirehoseProducer);
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void testBuildStreamTopology() {
        when(mockSerdeFactory.createJsonProtoSpanSerde(anyString())).thenReturn(mockSpanSerde);
        when(mockKafkaConsumerConfig.getFromTopic()).thenReturn(FROM_TOPIC);
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStream);

        protobufToFirehoseProducer.buildStreamTopology(mockKStreamBuilder);

        verify(mockSerdeFactory).createJsonProtoSpanSerde(APPLICATION);
        verify(mockKafkaConsumerConfig).getFromTopic();
        verify(mockKStreamBuilder).stream(any(Serdes.StringSerde.class), eq(mockSpanSerde), eq(FROM_TOPIC));
        verify(mockKStream).foreach(mockProduceIntoExternalKafkaAction);
    }
}
