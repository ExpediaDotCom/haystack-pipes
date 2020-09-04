package com.expedia.www.haystack.pipes.kafka.producer;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ServiceTest {

    @Mock
    private SerdeFactory mockSerdeFactory;
    @Mock
    private MetricRegistry mockMetricRegistry;
    @Mock
    private ProjectConfiguration mockProjectConfiguration;
    @Mock
    private HealthController mockHealthController;

    private Service service;

    @Before
    public void setUp(){
        service = new Service(mockSerdeFactory,mockMetricRegistry,
                ProjectConfiguration.getInstance(),mockHealthController);
    }

    @Test
    public void main() {
    }

    @Test
    public void getProtobufToKafkaProducer() {
    }

    @Test
    public void getKafkaStreamStarter() {
    }

    @Test
    public void testGetKafkaToKafkaPipeline() {
        assertEquals(KafkaToKafkaPipeline.class, service.getKafkaToKafkaPipeline().getClass());
    }

    @Test
    public void testInPlaceHealthCheck() {
        service.inPlaceHealthCheck();
        verify(mockHealthController).addListener(any());
    }

    @Test
    public void testGetJmxReporter() {
        assertEquals(JmxReporter.class, service.getJmxReporter().getClass());
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

