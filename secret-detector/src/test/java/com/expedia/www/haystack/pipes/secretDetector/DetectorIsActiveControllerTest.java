package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.www.haystack.pipes.secretDetector.DetectorIsActiveController.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;

import java.util.Set;

import static com.expedia.www.haystack.pipes.secretDetector.DetectorIsActiveController.STARTUP_MSG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DetectorIsActiveControllerTest {
    @Mock
    private Factory mockFactory;
    @Mock
    private Logger mockLogger;
    @Mock
    private SpringApplication mockSpringApplication;
    @Mock
    private DetectorProducer mockDetectorProducer;

    private Factory factory;

    @Before
    public void setUp() {
        storeKafkaProducerIsActiveControllerWithMocksInStaticInstance();
        factory = new Factory();
    }

    private void storeKafkaProducerIsActiveControllerWithMocksInStaticInstance() {
        new DetectorIsActiveController(mockDetectorProducer, mockFactory, mockLogger);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockFactory, mockLogger, mockSpringApplication, mockDetectorProducer);
        clearKafkaProducerIsActiveControllerInStaticInstance();
    }

    private void clearKafkaProducerIsActiveControllerInStaticInstance() {
        DetectorIsActiveController.INSTANCE.set(null);
    }

    @Test
    public void testMain() {
        when(mockFactory.createSpringApplication()).thenReturn(mockSpringApplication);

        final String[] args = new String[0];
        DetectorIsActiveController.main(args);

        verify(mockLogger).info(STARTUP_MSG);
        verify(mockFactory).createSpringApplication();
        verify(mockDetectorProducer).main();
        verify(mockSpringApplication).run(args);
    }

    @Test
    public void testFactoryCreateSpringApplication() {
        final SpringApplication springApplication = factory.createSpringApplication();

        final Set<Object> sources = springApplication.getSources();
        assertEquals(1, sources.size());
        final Object[] objects = sources.toArray();
        assertSame(DetectorIsActiveController.class, objects[0]);
    }
}
