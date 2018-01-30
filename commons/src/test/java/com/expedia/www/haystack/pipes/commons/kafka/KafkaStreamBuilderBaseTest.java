package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.ForeachAction;
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
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStreamBuilderBaseTest {
    private static final String APPLICATION = RANDOM.nextLong() + "APPLICATION";
    private static final String FROM_TOPIC = RANDOM.nextLong() + "FROM_TOPIC";

    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private SpanSerdeFactory mockSpanSerdeFactory;
    @Mock
    private KafkaConfigurationProvider mockKafkaConfigurationProvider;
    @Mock
    private ForeachAction<String, Span> mockForeachAction;
    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private Serde<Span> mockSerdeSpan;
    @Mock
    private KStream<String, Span> mockKStream;

    class KafkaStreamBuilderBaseImpl extends KafkaStreamBuilderBase {
        public KafkaStreamBuilderBaseImpl() {
            super(mockKafkaStreamStarter, mockSpanSerdeFactory, APPLICATION, mockKafkaConfigurationProvider,
                    mockForeachAction);
        }
    }

    private KafkaStreamBuilderBase kafkaStreamBuilderBase;

    @Before
    public void setUp() {
        kafkaStreamBuilderBase = new KafkaStreamBuilderBaseImpl();
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockKafkaStreamStarter, mockSpanSerdeFactory, mockKafkaConfigurationProvider,
                mockForeachAction, mockKStreamBuilder, mockSerdeSpan, mockKStream);
    }

    @Test
    public void testBuildStreamTopology() {
        when(mockSpanSerdeFactory.createSpanSerde(anyString())).thenReturn(mockSerdeSpan);
        when(mockKafkaConfigurationProvider.fromtopic()).thenReturn(FROM_TOPIC);
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStream);

        kafkaStreamBuilderBase.buildStreamTopology(mockKStreamBuilder);

        verify(mockSpanSerdeFactory).createSpanSerde(APPLICATION);
        verify(mockKafkaConfigurationProvider).fromtopic();
        verify(mockKStreamBuilder).stream(Matchers.<Serde<String>>any(), Matchers.<Serde<String>>any(), eq(FROM_TOPIC));
        verify(mockKStream).foreach(mockForeachAction);
    }

    @Test
    public void testMain() {
        kafkaStreamBuilderBase.main();

        verify(mockKafkaStreamStarter).createAndStartStream(kafkaStreamBuilderBase);
    }
}
