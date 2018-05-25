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
package com.expedia.www.haystack.pipes.secretDetector.mains;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.expedia.www.haystack.pipes.secretDetector.DetectorAction;
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
import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProtobufToDetectorActionTest {
    private static final String FROM_TOPIC = RANDOM.nextLong() + "FROM_TOPIC";

    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private SerdeFactory mockSerdeFactory;
    @Mock
    private DetectorAction mockDetectorAction;
    @Mock
    private KafkaConfigurationProvider mockKafkaConfigurationProvider;
    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private KStream<String, Span> mockKStream;
    @Mock
    private Serde<Span> mockSpanSerde;

    private ProtobufToDetectorAction protobufToDetectorAction;

    @Before
    public void setUp() {
        protobufToDetectorAction = new ProtobufToDetectorAction(
                mockKafkaStreamStarter, mockSerdeFactory, mockDetectorAction, mockKafkaConfigurationProvider);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockKafkaStreamStarter, mockSerdeFactory, mockDetectorAction,
                mockKafkaConfigurationProvider, mockKStreamBuilder, mockKStream, mockSpanSerde);
    }

    @Test
    public void testMain() {
        protobufToDetectorAction.main();

        verify(mockKafkaStreamStarter).createAndStartStream(protobufToDetectorAction);
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void testBuildStreamTopology() {
        when(mockSerdeFactory.createSpanSerde(anyString())).thenReturn(mockSpanSerde);
        when(mockKafkaConfigurationProvider.fromtopic()).thenReturn(FROM_TOPIC);
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStream);

        protobufToDetectorAction.buildStreamTopology(mockKStreamBuilder);

        verify(mockSerdeFactory).createSpanSerde(APPLICATION);
        verify(mockKafkaConfigurationProvider).fromtopic();
        verify(mockKStreamBuilder).stream(any(Serdes.StringSerde.class), eq(mockSpanSerde), eq(FROM_TOPIC));
        verify(mockKStream).foreach(mockDetectorAction);
    }
}
