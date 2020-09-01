package com.expedia.www.haystack.pipes.kafkaProducer.key.extractor;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.netflix.servo.util.VisibleForTesting;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class JsonExtractor implements SpanKeyExtractor {

    private final JsonFormat.Printer jsonPrinter = JsonFormat.printer().omittingInsignificantWhitespace();
    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(JsonExtractor.class);

    @Override
    public String name() {
        return "JsonExtractor";
    }

    @Override
    public void configure(Config config) {
        logger.info("{} class loaded with config: {}", JsonExtractor.class.getSimpleName(), config);
    }

    @Override
    public Optional<String> extract(Span span) {
        try {
            return Optional.of(jsonPrinter.print(span));
        } catch (InvalidProtocolBufferException e) {
            logger.error("Exception occurred while extracting span: " + e.getMessage());
        }
        return Optional.empty();
    }

    @Override
    public String getKey() {
        return "externalKafkaTopic";
    }

    @Override
    public List<String> getTopics() {
        return new ArrayList<>();
    }
}
