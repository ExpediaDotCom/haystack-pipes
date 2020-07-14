package com.expedia.www.haystack.pipes.kafkaProducer.extractor;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.SpanKeyExtractor;

public class SampleExtractor implements SpanKeyExtractor {

    @Override
    public String name() {
        return "SampleExtractor";
    }

    @Override
    public void configure() {
    }

    @Override
    public String extract(Span span) {
        return span.toString();
    }

    @Override
    public String getKey() {
        return "test-key";
    }

    @Override
    public String getTopic() {
        return "test-topic";
    }
}
