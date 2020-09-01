package com.expedia.www.haystack.pipes.producer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;

import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;


@RunWith(MockitoJUnitRunner.class)
public class ServiceTest {

    @Mock
    Logger mockLogger;

    private Service service;
    Logger realLogger;

    @Before
    public void setUp() throws Exception {
        service = new Service();
        realLogger = Service.logger;
        Service.logger = mockLogger;
    }

    @Test
    public void testMain() {
        service.main(new String[0]);
        verify(mockLogger).info("Starting KafkaToKafka Producer Pipeline");
    }
}