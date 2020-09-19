package com.expedia.www.haystack.pipes.kafka.producer;

import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;

import java.util.List;

/*
class wraps extractor with the producers
 */
public class KafkaProducerExtractorMapping {

    SpanKeyExtractor spanKeyExtractor;
    List<KafkaProducerWrapper> kafkaProducerWrappers;

    public KafkaProducerExtractorMapping(SpanKeyExtractor spanKeyExtractor, List<KafkaProducerWrapper> kafkaProducerWrappers) {
        this.spanKeyExtractor = spanKeyExtractor;
        this.kafkaProducerWrappers = kafkaProducerWrappers;
    }

    public SpanKeyExtractor getSpanKeyExtractor() {
        return spanKeyExtractor;
    }

    public List<KafkaProducerWrapper> getKafkaProducerWrappers() {
        return kafkaProducerWrappers;
    }
}
