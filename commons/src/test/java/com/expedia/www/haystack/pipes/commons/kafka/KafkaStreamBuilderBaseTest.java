package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
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
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStreamBuilderBaseTest {
    private static final String APPLICATION = RANDOM.nextLong() + "APPLICATION";
    private static final String FROM_TOPIC = RANDOM.nextLong() + "FROM_TOPIC";

    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private SerdeFactory mockSerdeFactory;
    @Mock
    private KafkaConsumerConfig mockKafkaConsumerConfig;
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
    private KafkaStreamBuilderBase kafkaStreamBuilderBaseForeachAction;
    private KafkaStreamBuilderBase kafkaStreamBuilderBaseProcessorSupplier;

    @Before
    public void setUp() {
        kafkaStreamBuilderBaseForeachAction = new KafkaStreamBuilderBaseForeachActionImpl();
        kafkaStreamBuilderBaseProcessorSupplier = new KafkaStreamBuilderBaseProcessorSupplierImpl();
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockKafkaStreamStarter, mockSerdeFactory, mockKafkaConsumerConfig,
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
        when(mockSerdeFactory.createJsonProtoSpanSerde(anyString())).thenReturn(mockSerdeSpan);
        when(mockKafkaConsumerConfig.getFromTopic()).thenReturn(FROM_TOPIC);
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStream);
    }

    private void verifiesForBuildStreamTopology() {
        verify(mockSerdeFactory).createJsonProtoSpanSerde(APPLICATION);
        verify(mockKafkaConsumerConfig).getFromTopic();
        verify(mockKStreamBuilder).stream(Matchers.<Serde<String>>any(), Matchers.<Serde<String>>any(), eq(FROM_TOPIC));
    }

    @Test
    public void testMain() {
        kafkaStreamBuilderBaseForeachAction.main();

        verify(mockKafkaStreamStarter).createAndStartStream(kafkaStreamBuilderBaseForeachAction);
    }

    class KafkaStreamBuilderBaseForeachActionImpl extends KafkaStreamBuilderBase {
        @SuppressWarnings("WeakerAccess")
        public KafkaStreamBuilderBaseForeachActionImpl() {
            super(mockKafkaStreamStarter, mockSerdeFactory, APPLICATION, mockKafkaConsumerConfig,
                    mockForeachAction);
        }
    }

    class KafkaStreamBuilderBaseProcessorSupplierImpl extends KafkaStreamBuilderBase {
        @SuppressWarnings("WeakerAccess")
        public KafkaStreamBuilderBaseProcessorSupplierImpl() {
            super(mockKafkaStreamStarter, mockSerdeFactory, APPLICATION, mockKafkaConsumerConfig,
                    mockProcessorSupplier);
        }
    }
}
