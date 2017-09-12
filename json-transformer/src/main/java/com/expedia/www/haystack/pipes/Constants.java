package com.expedia.www.haystack.pipes;

public interface Constants {
    String SUBSYSTEM = "pipes";
    String APPLICATION = "haystack-external-json-transformer";

    // TODO Move topics to a centralized location to be used by all services
    String KAFKA_FROM_TOPIC = "SpanObject-ProtobufFormat-Topic-1";
    String KAFKA_TO_TOPIC = "SpanObject-JsonFormat-Topic-3";
}
