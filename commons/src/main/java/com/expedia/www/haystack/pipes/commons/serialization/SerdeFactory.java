package com.expedia.www.haystack.pipes.commons.serialization;

import com.expedia.open.tracing.Span;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdeFactory {
    public Serde<Span> createJsonProtoSpanSerde(String application) {
        final SpanJsonSerializer spanJsonSerializer = new SpanJsonSerializer(application);
        final SpanProtobufDeserializer protobufDeserializer = new SpanProtobufDeserializer(application);
        return Serdes.serdeFrom(spanJsonSerializer, protobufDeserializer);
    }

    public Serde<Span> createProtoProtoSpanSerde(String application) {
        final SpanProtobufSerializer spanProtobufSerializer = new SpanProtobufSerializer(application);
        final SpanProtobufDeserializer protobufDeserializer = new SpanProtobufDeserializer(application);
        return Serdes.serdeFrom(spanProtobufSerializer, protobufDeserializer);
    }

    public Serde<String> createStringSerde() {
        return Serdes.String();
    }
}
