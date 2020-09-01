package com.expedia.www.haystack.pipes.kafkaProducer.key.extractor;

import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class JsonExtractorTest {

    @Mock
    Logger mockLogger;
    @Mock
    Config mockConfig;

    private JsonExtractor jsonExtractor;

    @Before
    public void setUp(){
        jsonExtractor = new JsonExtractor();
    }

    @Test
    public void testName() {
        assertEquals(JsonExtractor.class.getSimpleName(), jsonExtractor.name());
    }

    @Test
    public void testConfigure() {
        Logger realLogger = JsonExtractor.logger;
        JsonExtractor.logger = mockLogger;
        jsonExtractor.configure(mockConfig);
        verify(mockLogger).info("{} class loaded with config: {}", JsonExtractor.class.getSimpleName(), mockConfig);
    }

    @Test
    public void extract() {
        assertEquals(JSON_SPAN_STRING, jsonExtractor.extract(FULLY_POPULATED_SPAN).get());
    }

    @Test
    public void getKey() {
        assertEquals("externalKafkaTopic", jsonExtractor.getKey());
    }
}