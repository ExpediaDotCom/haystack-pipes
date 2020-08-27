package com.expedia.www.haystack.pipes.kafka.producer.extractor;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SampleExtractor implements SpanKeyExtractor {

    private static Logger logger = LoggerFactory.getLogger(SampleExtractor.class);

    @Override
    public String name() {
        return "SampleExtractor";
    }

    @Override
    public void configure(Config config) {
        logger.debug("{} got config: {}", name(), config);
    }

    @Override
    public Optional<String> extract(Span span) {
        return Optional.empty();
    }

    @Override
    public String getKey() {
        return "dummy-key";
    }

    @Override
    public List<String> getTopics() {
        return new ArrayList<>();
    }
}
