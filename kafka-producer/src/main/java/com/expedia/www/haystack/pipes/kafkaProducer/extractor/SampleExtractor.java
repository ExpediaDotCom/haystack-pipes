package com.expedia.www.haystack.pipes.kafkaProducer.extractor;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.SpanKeyExtractor;
import com.google.protobuf.InvalidProtocolBufferException;

public class SampleExtractor implements SpanKeyExtractor {
    @Override
    public String name() {
        return null;
    }

    @Override
    public void configure() {

    }

    @Override
    public String extract(Span span) {
        System.out.println("reached here----------------------------------------------------------");
        return null;
    }
}
