package com.expedia.www.haystack.pipes.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.producer.key.extractor.JsonExtractor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaToKafkaPipelineTest {

    private KafkaToKafkaPipeline kafkaToKafkaPipeline;
    private static final String TOPIC = RANDOM.nextLong() + "TOPIC";
    private static final String KEY = RANDOM.nextLong() + "KEY";
    private static final String VALUE = RANDOM.nextLong() + "VALUE";

    @Mock
    private MetricRegistry mockMetricRegistry;
    @Mock
    private Counter mockRequestCounter;
    @Mock
    private Logger mockLogger;
    @Mock
    private Timer mockKafkaProducerTimer;
    @Mock
    Timer.Context mockTimer;


    @Before
    public void setUp() throws Exception {
        List<SpanKeyExtractor> spanKeyExtractors = new ArrayList<>();
        spanKeyExtractors.add(new JsonExtractor());
        when(mockMetricRegistry.counter(anyString())).thenReturn(mockRequestCounter);
        when(mockMetricRegistry.timer(anyString())).thenReturn(mockKafkaProducerTimer);
        when(mockKafkaProducerTimer.time()).thenReturn(mockTimer);
        kafkaToKafkaPipeline = new KafkaToKafkaPipeline(mockMetricRegistry,
                ProjectConfiguration.getInstance(),
                spanKeyExtractors);
        KafkaToKafkaPipeline.logger = mockLogger;

    }

    @Test
    public void testApplyWithTags() {
        kafkaToKafkaPipeline.apply(null, FULLY_POPULATED_SPAN);

        verify(mockRequestCounter).inc();
        verify(mockLogger).info("KafkaProducer sending message: {},with key: {}  ", JSON_SPAN_STRING_WITH_FLATTENED_TAGS, "externalKafkaTopic");
    }

    @Test
    public void testFactoryCreateProducerRecord() {
        final ProducerRecord<String, String> producerRecord = KafkaToKafkaPipeline.factory.createProducerRecord(TOPIC, KEY, VALUE);
        assertEquals(KEY, producerRecord.key());
        assertEquals(VALUE, producerRecord.value());
    }
}