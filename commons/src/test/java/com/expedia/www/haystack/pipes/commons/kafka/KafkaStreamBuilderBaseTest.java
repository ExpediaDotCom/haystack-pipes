package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.ProcessorSupplier;
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
    private SerdeFactory mockSerdeFactory;
    @Mock
    private KafkaConfigurationProvider mockKafkaConfigurationProvider;
    @Mock
    private ForeachAction<String, Span> mockForeachAction;
    @Mock
    private ProcessorSupplier<String, Span> mockProcessorSupplier;
    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private Serde<Span> mockSerdeSpan;
    @Mock
    private KStream<String, Span> mockKStream;

    class KafkaStreamBuilderBaseForeachActionImpl extends KafkaStreamBuilderBase {
        @SuppressWarnings("WeakerAccess")
        public KafkaStreamBuilderBaseForeachActionImpl() {
            super(mockKafkaStreamStarter, mockSerdeFactory, APPLICATION, mockKafkaConfigurationProvider,
                    mockForeachAction);
        }
    }

    class KafkaStreamBuilderBaseProcessorSupplierImpl extends KafkaStreamBuilderBase {
        @SuppressWarnings("WeakerAccess")
        public KafkaStreamBuilderBaseProcessorSupplierImpl() {
            super(mockKafkaStreamStarter, mockSerdeFactory, APPLICATION, mockKafkaConfigurationProvider,
                    mockProcessorSupplier);
        }
    }

    private KafkaStreamBuilderBase kafkaStreamBuilderBaseForeachAction;
    private KafkaStreamBuilderBase kafkaStreamBuilderBaseProcessorSupplier;

    @Before
    public void setUp() {
        kafkaStreamBuilderBaseForeachAction = new KafkaStreamBuilderBaseForeachActionImpl();
        kafkaStreamBuilderBaseProcessorSupplier = new KafkaStreamBuilderBaseProcessorSupplierImpl();
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockKafkaStreamStarter, mockSerdeFactory, mockKafkaConfigurationProvider,
                mockForeachAction, mockKStreamBuilder, mockSerdeSpan, mockKStream);
    }

    @Test
    public void testBuildStreamTopologyForeachAction() {
        whensForBuildStreamTopology();

        kafkaStreamBuilderBaseForeachAction.buildStreamTopology(mockKStreamBuilder);

        verifiesForBuildStreamTopology();
        verify(mockKStream).foreach(mockForeachAction);
    }

    @Test
    public void testBuildStreamTopologyProcessorSupplier() {
        whensForBuildStreamTopology();

        kafkaStreamBuilderBaseProcessorSupplier.buildStreamTopology(mockKStreamBuilder);

        verifiesForBuildStreamTopology();
        verify(mockKStream).process(mockProcessorSupplier);
    }

    private void whensForBuildStreamTopology() {
        when(mockSerdeFactory.createSpanSerde(anyString())).thenReturn(mockSerdeSpan);
        when(mockKafkaConfigurationProvider.fromtopic()).thenReturn(FROM_TOPIC);
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStream);
    }

    private void verifiesForBuildStreamTopology() {
        verify(mockSerdeFactory).createSpanSerde(APPLICATION);
        verify(mockKafkaConfigurationProvider).fromtopic();
        verify(mockKStreamBuilder).stream(Matchers.<Serde<String>>any(), Matchers.<Serde<String>>any(), eq(FROM_TOPIC));
    }

    @Test
    public void testMain() {
        kafkaStreamBuilderBaseForeachAction.main();

        verify(mockKafkaStreamStarter).createAndStartStream(kafkaStreamBuilderBaseForeachAction);
    }
}
