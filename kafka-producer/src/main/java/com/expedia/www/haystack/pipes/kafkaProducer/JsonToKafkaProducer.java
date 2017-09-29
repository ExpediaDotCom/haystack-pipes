package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.www.haystack.pipes.commons.Configuration;
import org.cfg4j.provider.ConfigurationProvider;

public class JsonToKafkaProducer {
    static final String CLIENT_ID = "haystack-pipes-json-to-kafka-producer";
    static final String STARTED_MSG = "Now started Producer stream to send JSON spans to external Kafka";
    private static final Configuration CONFIGURATION = new Configuration();
    private static final ConfigurationProvider CONFIGURATION_PROVIDER = CONFIGURATION.createMergeConfigurationProvider();
}
