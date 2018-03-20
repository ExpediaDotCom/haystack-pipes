package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
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
public class DetectorProducerTest {
    private final static String FROM_TOPIC = RANDOM.nextLong() + "FROM_TOPIC";

    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private SpanSerdeFactory mockSpanSerdeFactory;
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

    private DetectorProducer detectorProducer;

    @Before
    public void setUp() {
        detectorProducer = new DetectorProducer(
                mockKafkaStreamStarter, mockSpanSerdeFactory, mockDetectorAction, mockKafkaConfigurationProvider);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockKafkaStreamStarter, mockSpanSerdeFactory, mockDetectorAction,
                mockKafkaConfigurationProvider, mockKStreamBuilder, mockKStream, mockSpanSerde);
    }

    @Test
    public void testMain() {
        detectorProducer.main();

        verify(mockKafkaStreamStarter).createAndStartStream(detectorProducer);
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void testBuildStreamTopology() {
        when(mockSpanSerdeFactory.createSpanSerde(anyString())).thenReturn(mockSpanSerde);
        when(mockKafkaConfigurationProvider.fromtopic()).thenReturn(FROM_TOPIC);
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStream);

        detectorProducer.buildStreamTopology(mockKStreamBuilder);

        verify(mockSpanSerdeFactory).createSpanSerde(APPLICATION);
        verify(mockKafkaConfigurationProvider).fromtopic();
        verify(mockKStreamBuilder).stream(any(Serdes.StringSerde.class), eq(mockSpanSerde), eq(FROM_TOPIC));
        verify(mockKStream).foreach(mockDetectorAction);
    }
}
