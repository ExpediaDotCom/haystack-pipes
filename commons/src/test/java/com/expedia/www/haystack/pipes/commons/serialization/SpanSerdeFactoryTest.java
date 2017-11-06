package com.expedia.www.haystack.pipes.commons.serialization;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.serialization.SpanJsonSerializer;
import com.expedia.www.haystack.pipes.commons.serialization.SpanProtobufDeserializer;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SpanSerdeFactoryTest {
    private final static Random RANDOM = new Random();
    private final static String APPLICATION = RANDOM.nextLong() + "APPLICATION";

    private SpanSerdeFactory spanSerdeFactory;

    @Before
    public void setUp() {
        spanSerdeFactory = new SpanSerdeFactory();
    }

    @Test
    public void testCreateSpanSerde() {
        final Serde<Span> spanSerde = spanSerdeFactory.createSpanSerde(APPLICATION);

        final SpanProtobufDeserializer deserializer = (SpanProtobufDeserializer) spanSerde.deserializer();
        assertEquals(APPLICATION, deserializer.application);
        final SpanJsonSerializer serializer = (SpanJsonSerializer) spanSerde.serializer();
        assertEquals(APPLICATION, serializer.application);
    }
}
