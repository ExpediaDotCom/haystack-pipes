package com.expedia.www.haystack.pipes.kafkaProducer.extractor;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.SpanKeyExtractor;


public class SampleExtractor implements SpanKeyExtractor {

    @java.lang.Override
    public String name() {
        return "SampleExtractor";
    }

    @java.lang.Override
    public void configure() {
    }

    @java.lang.Override
    public String extract(Span span) {
        return span.toString();
    }

    @java.lang.Override
    public String getKey() {
        return "test-key";
    }

    @java.lang.Override
    public String getTopic() {
        return "test-topic";
    }
}
