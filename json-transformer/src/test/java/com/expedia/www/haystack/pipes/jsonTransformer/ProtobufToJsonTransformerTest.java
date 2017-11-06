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
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.Metrics;
import com.expedia.www.haystack.pipes.commons.serialization.SpanJsonSerializer;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProtobufToJsonTransformerTest {
    @Mock
    private Metrics mockMetrics;
    private Metrics realMetrics;

    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private KStream<String, Span> mockKStreamStringSpan;
    @Mock
    private KStream<String, SpanJsonSerializer> mockKStreamStringSpanJsonSerializer;

    private ProtobufToJsonTransformer protobufToJsonTransformer;

    @Before
    public void setUp() {
        realMetrics = ProtobufToJsonTransformer.metrics;
        ProtobufToJsonTransformer.metrics = mockMetrics;
        protobufToJsonTransformer = new ProtobufToJsonTransformer(mockKafkaStreamStarter, new SpanSerdeFactory());
    }

    @After
    public void tearDown() {
        ProtobufToJsonTransformer.metrics = realMetrics;
        verifyNoMoreInteractions(mockMetrics, mockKafkaStreamStarter, mockKStreamBuilder, mockKStreamStringSpan,
                mockKStreamStringSpanJsonSerializer);
    }

    @Test
    public void testDefaultConstructor() {
        protobufToJsonTransformer = new ProtobufToJsonTransformer();

        assertEquals(ProtobufToJsonTransformer.CLIENT_ID, protobufToJsonTransformer.kafkaStreamStarter.clientId);
        assertEquals(ProtobufToJsonTransformer.class, protobufToJsonTransformer.kafkaStreamStarter.containingClass);
    }

    @Test
    public void testMain() {
        protobufToJsonTransformer.main();

        verify(mockMetrics).startMetricsPolling();
        verify(mockKafkaStreamStarter).createAndStartStream(protobufToJsonTransformer);
    }

    @Test
    public void testBuildStreamTopology() {
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStreamStringSpan);
        when(mockKStreamStringSpan.mapValues(Matchers.<ValueMapper<Span, SpanJsonSerializer>>any()))
                .thenReturn(mockKStreamStringSpanJsonSerializer);
        protobufToJsonTransformer.buildStreamTopology(mockKStreamBuilder);

        verify(mockKStreamBuilder).stream(any(), Matchers.<Serde<Span>>any(), eq("proto-spans"));
        verify(mockKStreamStringSpan).mapValues(any());
        verify(mockKStreamStringSpanJsonSerializer).to(any(),  any(), eq("json-spans"));
    }
}
