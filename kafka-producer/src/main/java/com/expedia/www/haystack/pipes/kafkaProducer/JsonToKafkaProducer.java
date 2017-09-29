package com.expedia.www.haystack.pipes.kafkaProducer;

import org.cfg4j.provider.ConfigurationProvider;

import static com.expedia.www.haystack.pipes.commons.Configuration.createMergeConfigurationProvider;

public class JsonToKafkaProducer {
    static final String CLIENT_ID = "haystack-pipes-json-to-kafka-producer";
    static final String STARTED_MSG = "Now started Producer stream to send JSON spans to external Kafka";
    private static final ConfigurationProvider CONFIGURATION_PROVIDER = createMergeConfigurationProvider();
}
