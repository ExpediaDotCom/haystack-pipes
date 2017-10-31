package com.expedia.www.haystack.pipes.commons;

import com.expedia.open.tracing.Span;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SpanSerdeFactory {
    public Serde<Span> createSpanSerde(String application) {
        final SpanProtobufDeserializer protobufDeserializer = new SpanProtobufDeserializer(application);
        final SpanJsonSerializer spanJsonSerializer = new SpanJsonSerializer(application);
        return Serdes.serdeFrom(spanJsonSerializer, protobufDeserializer);
    }
}
