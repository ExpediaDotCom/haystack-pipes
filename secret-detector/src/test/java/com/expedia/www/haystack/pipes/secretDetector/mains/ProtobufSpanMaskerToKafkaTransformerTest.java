package com.expedia.www.haystack.pipes.secretDetector.mains;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.commons.secretDetector.span.SpanSecretMasker;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProtobufSpanMaskerToKafkaTransformerTest {
    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private SpanSecretMasker mockSpanSecretMasker;
    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private KStream<String, Span> mockKStreamStringSpan;
    @Mock
    private KStream<String, SpanJsonSerializer> mockKStreamStringSpanJsonSerializer;

    private ProtobufSpanMaskerToKafkaTransformer protobufSpanMaskerToKafkaTransformer;

    @Before
    public void setUp() {
        protobufSpanMaskerToKafkaTransformer = new ProtobufSpanMaskerToKafkaTransformer(
                mockKafkaStreamStarter, new SpanSerdeFactory(), mockSpanSecretMasker);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockKafkaStreamStarter, mockSpanSecretMasker, mockKStreamBuilder,
                mockKStreamStringSpan, mockKStreamStringSpanJsonSerializer);
    }

    @Test
    public void testMain() {
        protobufSpanMaskerToKafkaTransformer.main();

        verify(mockKafkaStreamStarter).createAndStartStream(protobufSpanMaskerToKafkaTransformer);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBuildStreamTopology() {
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStreamStringSpan);
        when(mockKStreamStringSpan.mapValues(Matchers.<ValueMapper<Span, SpanJsonSerializer>>any()))
                .thenReturn(mockKStreamStringSpanJsonSerializer);
        final Span span = Span.getDefaultInstance();
        when(mockSpanSecretMasker.apply(any())).thenReturn(span);

        protobufSpanMaskerToKafkaTransformer.buildStreamTopology(mockKStreamBuilder);

        verify(mockKStreamBuilder).stream(any(), Matchers.<Serde<Span>>any(), eq("proto-spans"));
        ArgumentCaptor<ValueMapper> argumentCaptor = ArgumentCaptor.forClass(ValueMapper.class);
        verify(mockKStreamStringSpan).mapValues(argumentCaptor.capture());
        final ValueMapper<Span, Span> valueMapper = argumentCaptor.getValue();
        assertSame(span, valueMapper.apply(span));
        verify(mockSpanSecretMasker).apply(span);
        verify(mockKStreamStringSpanJsonSerializer).to(any(),  any(), eq("proto-spans-scrubbed"));
    }

}
