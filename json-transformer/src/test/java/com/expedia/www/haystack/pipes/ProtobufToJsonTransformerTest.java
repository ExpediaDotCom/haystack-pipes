package com.expedia.www.haystack.pipes;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.ProtobufToJsonTransformer.Factory;
import com.expedia.www.haystack.pipes.ProtobufToJsonTransformer;
import com.expedia.www.haystack.pipes.SpanJsonSerializer;
import com.expedia.www.haystack.pipes.SystemExitUncaughtExceptionHandler;
import com.netflix.servo.publish.PollScheduler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Properties;
import java.util.regex.Pattern;

import static com.expedia.www.haystack.pipes.Constants.KAFKA_FROM_TOPIC;
import static com.expedia.www.haystack.pipes.Constants.KAFKA_TO_TOPIC;
import static com.expedia.www.haystack.pipes.ProtobufToJsonTransformer.STARTED_MSG;
import static com.expedia.www.haystack.pipes.ProtobufToJsonTransformer.getProperties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProtobufToJsonTransformerTest {
    @Mock
    private Factory mockFactory;
    private Factory realFactory;

    @Mock
    private Logger mockLogger;
    private Logger realLogger;

    @Mock
    private KStreamBuilder mockKStreamBuilder;

    @Mock
    private KStream<String, Span> mockKStreamStringSpan;
    @Mock
    private KStream<String, SpanJsonSerializer> mockKStreamStringSpanJsonSerializer;
    @Mock
    private KafkaStreams mockKafkaStreams;
    @Mock
    private SystemExitUncaughtExceptionHandler mockSystemExitUncaughtExceptionHandler;

    @Before
    public void setUp() {
        realFactory = ProtobufToJsonTransformer.factory;
        ProtobufToJsonTransformer.factory = mockFactory;
        realLogger = ProtobufToJsonTransformer.logger;
        ProtobufToJsonTransformer.logger = mockLogger;
    }

    @After
    public void tearDown() {
        ProtobufToJsonTransformer.factory = realFactory;
        ProtobufToJsonTransformer.logger = realLogger;
        if (PollScheduler.getInstance().isStarted()) {
            PollScheduler.getInstance().stop();
        }
        verifyNoMoreInteractions(mockFactory, mockLogger, mockKStreamBuilder, mockKStreamStringSpan,
                mockKStreamStringSpanJsonSerializer, mockKafkaStreams, mockSystemExitUncaughtExceptionHandler);
    }

    @Test
    public void testMain() {
        when(mockFactory.createKStreamBuilder()).thenReturn(mockKStreamBuilder);
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStreamStringSpan);
        when(mockKStreamStringSpan.mapValues(Matchers.<ValueMapper<Span, SpanJsonSerializer>>any()))
                .thenReturn(mockKStreamStringSpanJsonSerializer);
        when(mockFactory.createKafkaStreams(any(KStreamBuilder.class), any(StreamsConfig.class)))
                .thenReturn(mockKafkaStreams);
        when(mockFactory.createSystemExitUncaughtExceptionHandler())
                .thenReturn(mockSystemExitUncaughtExceptionHandler);

        ProtobufToJsonTransformer.main(null);

        // The Serde objects are stateless, so verifying on any() is sufficient. The combination of Java generics and
        // method overloading means that sometimes you have to include generic information in any() and sometimes not.
        verify(mockFactory).createKStreamBuilder();
        verify(mockKStreamBuilder).stream(Matchers.any(), Matchers.<Serde<Span>>any(), eq(KAFKA_FROM_TOPIC));
        verify(mockKStreamStringSpan).mapValues(Matchers.any());
        verify(mockKStreamStringSpanJsonSerializer).to(Matchers.any(), Matchers.any(), eq(KAFKA_TO_TOPIC));
        verify(mockFactory).createKafkaStreams(eq(mockKStreamBuilder), eq(new StreamsConfig(getProperties())));
        verify(mockFactory).createSystemExitUncaughtExceptionHandler();
        verify(mockKafkaStreams).start();
        verify(mockKafkaStreams).setUncaughtExceptionHandler(mockSystemExitUncaughtExceptionHandler);
        verify(mockLogger).info(STARTED_MSG);
    }

    @Test
    public void testFactoryCreateKStreamBuilder() {
        assertNotNull(realFactory.createKStreamBuilder());
    }

    @Test
    public void testFactoryCreateKafkaStreams() {
        final Pattern emptyStringPattern = Pattern.compile("");
        when(mockKStreamBuilder.latestResetTopicsPattern()).thenReturn(emptyStringPattern);
        when(mockKStreamBuilder.earliestResetTopicsPattern()).thenReturn(emptyStringPattern);

        final Properties properties = getProperties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // don't go to the network
        assertNotNull(realFactory.createKafkaStreams(mockKStreamBuilder, new StreamsConfig(properties)));

        verify(mockKStreamBuilder).latestResetTopicsPattern();
        verify(mockKStreamBuilder).earliestResetTopicsPattern();
        verify(mockKStreamBuilder, times(2)).globalStateStores();
        verify(mockKStreamBuilder).buildGlobalStateTopology();
        verify(mockKStreamBuilder).sourceTopicPattern();
    }

    @Test
    public void testFactoryCreateSystemExitUncaughtExceptionHandler() {
        realFactory.createSystemExitUncaughtExceptionHandler();
    }

    @Test
    public void testGetProperties() {
        final Properties properties = getProperties();
        assertEquals(6, properties.size());
        assertEquals(ProtobufToJsonTransformer.CLIENT_ID, properties.get(StreamsConfig.CLIENT_ID_CONFIG));
        assertEquals(ProtobufToJsonTransformer.KLASS_NAME, properties.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(ProtobufToJsonTransformer.KLASS_SIMPLE_NAME, properties.get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("haystack.local:9092", properties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(1, properties.get(StreamsConfig.REPLICATION_FACTOR_CONFIG));
        assertEquals(WallclockTimestampExtractor.class, properties.get(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG));
    }

    @Test
    public void testDefaultConstructor() {
        new ProtobufToJsonTransformer();
    }
}
