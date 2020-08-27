package com.expedia.www.haystack.pipes.kafka.producer.key.extractor;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.config.ProjectConfiguration;
import com.expedia.www.haystack.pipes.commons.kafka.config.SpanKeyExtractorConfig;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Optional;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultExtractorTest {

    @Mock
    private Logger mockLogger;

    @Mock
    JsonFormat.Printer mockPrinter;

    private Logger realLogger;
    private SpanKeyExtractor defaultExtractor;

    @Before
    public void setUp() {
        defaultExtractor = new DefaultExtractor();
        realLogger = DefaultExtractor.logger;
        DefaultExtractor.logger = mockLogger;
    }

    @Test
    public void testName() {
        assertEquals("DefaultExtractor", defaultExtractor.name());
    }

    @Test
    public void testConfigure() {
        verifyConfigureExtractor();
        DefaultExtractor.logger = realLogger;
    }

    @Test
    public void testExtract() {
        assertEquals(Optional.of(JSON_SPAN_STRING), defaultExtractor.extract(FULLY_POPULATED_SPAN));
    }

    @Test
    public void testExtractException() throws InvalidProtocolBufferException {
        Span span = Span.newBuilder().build();
        JsonFormat.Printer realPrinter = DefaultExtractor.jsonPrinter;
        DefaultExtractor.jsonPrinter = mockPrinter;
        final InvalidProtocolBufferException exception = new InvalidProtocolBufferException(EXCEPTION_MESSAGE);
        when(mockPrinter.print(any(Span.class))).thenThrow(exception);
        assertEquals(Optional.empty(), defaultExtractor.extract(span));
        DefaultExtractor.jsonPrinter = realPrinter;
    }

    @Test
    public void testGetKey() {
        defaultExtractor.extract(FULLY_POPULATED_SPAN);
        assertEquals(FULLY_POPULATED_SPAN.getTraceId(), defaultExtractor.getKey());
    }

    @Test
    public void testGetTopics() {
        verifyConfigureExtractor();
        assertEquals(1, defaultExtractor.getTopics().size());
    }

    private void verifyConfigureExtractor() {
        SpanKeyExtractorConfig spanKeyExtractorConfig = ProjectConfiguration.getInstance().getKafkaProducerConfig().getSpanKeyExtractorConfig();
        defaultExtractor.configure(spanKeyExtractorConfig.getExtractorConfig().get(defaultExtractor.name()));
        Mockito.verify(mockLogger).debug("{} got config: {}", defaultExtractor.name(), spanKeyExtractorConfig.getExtractorConfig().get(defaultExtractor.name()));
    }


}