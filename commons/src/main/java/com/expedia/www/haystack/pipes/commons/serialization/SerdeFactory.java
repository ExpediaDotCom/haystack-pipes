package com.expedia.www.haystack.pipes.commons.serialization;

import com.expedia.open.tracing.Span;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdeFactory {
    public Serde<Span> createSpanSerde(String application) {
        final SpanProtobufDeserializer protobufDeserializer = new SpanProtobufDeserializer(application);
        final SpanJsonSerializer spanJsonSerializer = new SpanJsonSerializer(application);
        return Serdes.serdeFrom(spanJsonSerializer, protobufDeserializer);
    }

    public Serde<String> createStringSerde() {
        return Serdes.String();
    }
}
