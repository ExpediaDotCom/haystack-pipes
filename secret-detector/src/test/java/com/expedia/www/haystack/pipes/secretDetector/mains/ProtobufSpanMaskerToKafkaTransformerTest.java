package com.expedia.www.haystack.pipes.secretDetector.mains;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.commons.secretDetector.span.SpanSecretMasker;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
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

import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
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
    private KStream<String, Span> mockKStream;
    @Mock
    private SerdeFactory mockSerdeFactory;
    @Mock
    private Serde<String> mockFromStringSerde;
    @Mock
    private Serde<String> mockToStringSerde;
    @Mock
    private Serde<Span> mockFromSpanSerde;
    @Mock
    private Serde<Span> mockToSpanSerde;

    private ProtobufSpanMaskerToKafkaTransformer protobufSpanMaskerToKafkaTransformer;

    @Before
    public void setUp() {
        protobufSpanMaskerToKafkaTransformer = new ProtobufSpanMaskerToKafkaTransformer(
                mockKafkaStreamStarter, mockSerdeFactory, mockSpanSecretMasker);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockKafkaStreamStarter, mockSpanSecretMasker, mockKStreamBuilder, mockKStream,
                mockSerdeFactory, mockFromStringSerde, mockToStringSerde, mockFromSpanSerde, mockToSpanSerde);
    }

    @Test
    public void testMain() {
        protobufSpanMaskerToKafkaTransformer.main();

        verify(mockKafkaStreamStarter).createAndStartStream(protobufSpanMaskerToKafkaTransformer);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBuildStreamTopology() {
        when(mockSerdeFactory.createStringSerde()).thenReturn(mockFromStringSerde, mockToStringSerde);
        when(mockSerdeFactory.createProtoProtoSpanSerde(anyString())).thenReturn(mockFromSpanSerde, mockToSpanSerde);
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStream);
        when(mockKStream.mapValues(Matchers.<ValueMapper<Span, Span>>any()))
                .thenReturn(mockKStream);
        final Span span = Span.getDefaultInstance();
        when(mockSpanSecretMasker.apply(any())).thenReturn(span);

        protobufSpanMaskerToKafkaTransformer.buildStreamTopology(mockKStreamBuilder);

        verify(mockSerdeFactory, times(2)).createStringSerde();
        verify(mockSerdeFactory, times(2)).createProtoProtoSpanSerde(APPLICATION);
        verify(mockKStreamBuilder).stream(mockFromStringSerde, mockFromSpanSerde, "proto-spans");
        ArgumentCaptor<ValueMapper> argumentCaptor = ArgumentCaptor.forClass(ValueMapper.class);
        verify(mockKStream).mapValues(argumentCaptor.capture());
        final ValueMapper<Span, Span> valueMapper = argumentCaptor.getValue();
        assertSame(span, valueMapper.apply(span));
        verify(mockSpanSecretMasker).apply(span);
        verify(mockKStream).to(mockToStringSerde, mockToSpanSerde, "proto-spans-scrubbed");
    }

}
