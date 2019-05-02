package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.open.tracing.Span;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Optional;

public interface SpanProcessor {
    void init(TopicPartition topicPartition);
    void close();
    Optional<Long> process(ConsumerRecord<String, Span> rec);
}
