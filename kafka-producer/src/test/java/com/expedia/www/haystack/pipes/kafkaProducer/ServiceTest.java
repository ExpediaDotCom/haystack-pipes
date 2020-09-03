package com.expedia.www.haystack.pipes.kafkaProducer;

import com.codahale.metrics.JmxReporter;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ServiceTest {

    @Mock
    private ProtobufToKafkaProducer mockProtobufToKafkaProducer;
    @Mock
    private JmxReporter mockJmxReporter;

    private Service service;

    @Test
    public void testStartService() {
        ProtobufToKafkaProducer realProtobufToKafkaProducer = Service.protobufToKafkaProducer;
        Service.startService(mockProtobufToKafkaProducer);
        verify(mockProtobufToKafkaProducer).main();
    }

    @Test
    public void testGetExtractorKafkaProducerMap() {
        ProjectConfiguration projectConfiguration = ProjectConfiguration.getInstance();
        Map<SpanKeyExtractor, List<KafkaProducer<String, String>>> extractorProducerMap = Service.getExtractorKafkaProducerMap(projectConfiguration);
        assertEquals(extractorProducerMap.size(), 1);
    }

    @Test
    public void testGetExtractorKafkaProducerMapForIdempotent() {
        ProjectConfiguration projectConfiguration = ProjectConfiguration.getInstance();
        Map<SpanKeyExtractor, List<KafkaProducer<String, String>>> extractorProducerMap = Service.getExtractorKafkaProducerMap(projectConfiguration);
        assertEquals(extractorProducerMap, Service.getExtractorKafkaProducerMap(projectConfiguration));
    }
}