package com.expedia.www.haystack.pipes.kafka.producer;

import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class KafkaProducerExtractorMappingTest {

    @Mock
    SpanKeyExtractor mockSpanKeyExtractor;

    @Mock
    KafkaProducerWrapper mockKafkaProducerWrapper;

    KafkaProducerExtractorMapping kafkaProducerExtractorMapping;

    @Before
    public void setup() {
        kafkaProducerExtractorMapping = new KafkaProducerExtractorMapping(mockSpanKeyExtractor,
                Arrays.asList(mockKafkaProducerWrapper));
    }

    @Test
    public void testGetSpanKeyExtractor() {
        assertEquals(mockSpanKeyExtractor, kafkaProducerExtractorMapping.getSpanKeyExtractor());
    }

    @Test
    public void testGetKafkaProducerWrappers() {
        assertEquals(Arrays.asList(mockKafkaProducerWrapper), kafkaProducerExtractorMapping.getKafkaProducerWrappers());
    }
}