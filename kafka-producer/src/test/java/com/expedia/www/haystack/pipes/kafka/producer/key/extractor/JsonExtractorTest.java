/*
 * Copyright 2020 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.kafka.producer.key.extractor;

import com.expedia.www.haystack.pipes.kafka.producer.ProjectConfiguration;
import com.expedia.www.haystack.pipes.key.extractor.Record;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Collections;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JsonExtractorTest {

    @Mock
    Logger mockLogger;
    @Mock
    Config mockConfig;
    @Mock
    JsonFormat.Printer mockJsonPrinter;

    private JsonExtractor jsonExtractor;

    @Before
    public void setUp() {
        jsonExtractor = new JsonExtractor();
        ProjectConfiguration projectConfiguration = ProjectConfiguration.getInstance();
        jsonExtractor.configure(projectConfiguration.getSpanExtractorConfigs().get(jsonExtractor.name()));
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
        JsonExtractor.logger = realLogger;
        verify(mockLogger).info("{} class loaded with config: {}", JsonExtractor.class.getSimpleName(), mockConfig);
    }

    @Test
    public void testExtract() {
        Record record = jsonExtractor.getRecords(FULLY_POPULATED_SPAN).get(0);
        assertEquals("externalKafkaKey", record.getKey());
        assertEquals(JSON_SPAN_STRING, record.getMessage());
    }

    @Test
    public void testExtractWithException() throws InvalidProtocolBufferException {
        JsonFormat.Printer realPrinter = jsonExtractor.jsonPrinter;
        jsonExtractor.jsonPrinter = mockJsonPrinter;
        when(mockJsonPrinter.print(any())).thenThrow(new InvalidProtocolBufferException("mock Exception"));
        assertEquals(Collections.EMPTY_LIST, jsonExtractor.getRecords(FULLY_POPULATED_SPAN));
        assertEquals(0, jsonExtractor.getRecords(FULLY_POPULATED_SPAN).size());
        jsonExtractor.jsonPrinter = realPrinter;
    }
}