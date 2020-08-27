package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.pipes.commons.kafka.config.HttpPostConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.httpPoster.HttpPostConfigTest.LARGEST_POSSIBLE_MAX_BYTES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ContentCollectorTest {
    private static final String SMALLEST_POSSIBLE_JSON = "{}";
    private static final int SMALLEST_POSSIBLE_MAX_BYTES = SMALLEST_POSSIBLE_JSON.length() + "[]".length();
    private static final String PERIOD = ".";
    private static final String SPACES = "          ";
    private static final String NEW_LINE = "\n";
    private static final String BODY_PREFIX = "[";
    private static final String BODY_SUFFIX = "]";
    private static final String SEPARATOR = ",";

    @Mock
    private HttpPostConfig mockHttpPostConfig;

    private ContentCollector contentCollector;
    private int timesConstructorCalled = 1;

    @Before
    public void setUp() {
        when(mockHttpPostConfig.getMaxBytes()).thenReturn(LARGEST_POSSIBLE_MAX_BYTES);
        when(mockHttpPostConfig.getSeparator()).thenReturn(SEPARATOR);
        when(mockHttpPostConfig.getBodyPrefix()).thenReturn(BODY_PREFIX);
        when(mockHttpPostConfig.getBodySuffix()).thenReturn(BODY_SUFFIX);
        contentCollector = new ContentCollector(mockHttpPostConfig);
    }

    @After
    public void tearDown() {
        verify(mockHttpPostConfig, times(timesConstructorCalled)).getMaxBytes();
        verify(mockHttpPostConfig, times(timesConstructorCalled)).getSeparator();
        verify(mockHttpPostConfig, times(timesConstructorCalled)).getBodyPrefix();
        verify(mockHttpPostConfig, times(timesConstructorCalled)).getBodySuffix();
        verifyNoMoreInteractions(mockHttpPostConfig);
    }

    @Test
    public void testAddAndReturnBatchOneRecord() {
        timesConstructorCalled = 2;
        when(mockHttpPostConfig.getMaxBytes()).thenReturn(Integer.toString(SMALLEST_POSSIBLE_MAX_BYTES));
        contentCollector = new ContentCollector(mockHttpPostConfig);

        assertEquals("", contentCollector.addAndReturnBatch(SMALLEST_POSSIBLE_JSON));
        assertEquals('[' + SMALLEST_POSSIBLE_JSON + ']', contentCollector.addAndReturnBatch("."));
    }

    @Test
    public void testAddAndReturnBatchTwoRecords() {
        timesConstructorCalled = 2;
        final String expected = '[' + SMALLEST_POSSIBLE_JSON + ',' + SMALLEST_POSSIBLE_JSON + ']';
        when(mockHttpPostConfig.getMaxBytes()).thenReturn(Integer.toString(expected.length()));
        contentCollector = new ContentCollector(mockHttpPostConfig);

        assertEquals("", contentCollector.addAndReturnBatch(SPACES));
        assertEquals("", contentCollector.addAndReturnBatch(NEW_LINE));
        assertEquals("", contentCollector.addAndReturnBatch(SMALLEST_POSSIBLE_JSON));
        assertEquals("", contentCollector.addAndReturnBatch(SPACES));
        assertEquals("", contentCollector.addAndReturnBatch(NEW_LINE));
        assertEquals("", contentCollector.addAndReturnBatch(SMALLEST_POSSIBLE_JSON));
        assertEquals("", contentCollector.addAndReturnBatch(SPACES));
        assertEquals("", contentCollector.addAndReturnBatch(NEW_LINE));
        assertEquals(expected, contentCollector.addAndReturnBatch(PERIOD));
    }

}
