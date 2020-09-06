package com.expedia.www.haystack.pipes.kafka.producer;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ServiceTest {

    @Mock
    private SerdeFactory mockSerdeFactory;
    @Mock
    private MetricRegistry mockMetricRegistry;
    @Mock
    private Logger mockLogger;
    @Mock
    private HealthController mockHealthController;
    @Mock
    private ProtobufToKafkaProducer mockProtobufToKafkaProducer;
    @Mock
    private Service mockService;
    @Mock
    private JmxReporter mockJmxReporter;

    private Service service;

    @Before
    public void setUp() {
        service = Service.getInstance();
    }

    @Test
    public void testMain() throws Exception {
        Logger realLogger = Service.logger;
        Service.logger = mockLogger;
        Service realService = Service.service;
        Service.service = mockService;
        when(mockService.getProtobufToKafkaProducer(any())).thenReturn(mockProtobufToKafkaProducer);
        when(mockService.getJmxReporter()).thenReturn(mockJmxReporter);
        Service.main(new String[0]);
        Service.service = realService;
        Service.logger = realLogger;
        verify(mockLogger).info("Initializing Kafka Consumers");
        verify(mockProtobufToKafkaProducer).main();
    }

    @Test
    public void testGetInstance() {
        Service service = Service.getInstance();
        assertEquals(service, Service.getInstance());
    }


    @Test
    public void testGetKafkaStreamStarter() throws Exception {
        KafkaStreamStarter kafkaStreamStarter = service.getKafkaStreamStarter();
        assertEquals(ProtobufToKafkaProducer.class, kafkaStreamStarter.containingClass);
    }

    @Test
    public void testInPlaceHealthCheck() {
        HealthController realHealthController = Service.healthController;
        Service.healthController = mockHealthController;
        service.inPlaceHealthCheck();
        Service.healthController = realHealthController;
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

