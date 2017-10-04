package com.expedia.www.haystack.pipes.commons;

import org.apache.kafka.streams.kstream.KStreamBuilder;

public interface KafkaStreamBuilder {
    void buildStreamTopology(KStreamBuilder kStreamBuilder);
}
