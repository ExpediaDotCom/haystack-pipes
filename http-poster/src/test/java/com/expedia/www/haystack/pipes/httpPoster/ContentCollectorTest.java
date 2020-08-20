package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.pipes.commons.kafka.config.HttpPostConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.httpPoster.HttpPostConfigurationProviderTest.LARGEST_POSSIBLE_MAX_BYTES;
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
    private HttpPostConfig mockHttpPostConfigurationProvider;

    private ContentCollector contentCollector;
    private int timesConstructorCalled = 1;

    @Before
    public void setUp() {
        when(mockHttpPostConfigurationProvider.getMaxBytes()).thenReturn(LARGEST_POSSIBLE_MAX_BYTES);
        when(mockHttpPostConfigurationProvider.getSeparator()).thenReturn(SEPARATOR);
        when(mockHttpPostConfigurationProvider.getBodyPrefix()).thenReturn(BODY_PREFIX);
        when(mockHttpPostConfigurationProvider.getBodySuffix()).thenReturn(BODY_SUFFIX);
        contentCollector = new ContentCollector(mockHttpPostConfigurationProvider);
    }

    @After
    public void tearDown() {
        verify(mockHttpPostConfigurationProvider, times(timesConstructorCalled)).getMaxBytes();
        verify(mockHttpPostConfigurationProvider, times(timesConstructorCalled)).getSeparator();
        verify(mockHttpPostConfigurationProvider, times(timesConstructorCalled)).getBodyPrefix();
        verify(mockHttpPostConfigurationProvider, times(timesConstructorCalled)).getBodySuffix();
        verifyNoMoreInteractions(mockHttpPostConfigurationProvider);
    }

    @Test
    public void testAddAndReturnBatchOneRecord() {
        timesConstructorCalled = 2;
        when(mockHttpPostConfigurationProvider.getMaxBytes()).thenReturn(Integer.toString(SMALLEST_POSSIBLE_MAX_BYTES));
        contentCollector = new ContentCollector(mockHttpPostConfigurationProvider);

        assertEquals("", contentCollector.addAndReturnBatch(SMALLEST_POSSIBLE_JSON));
        assertEquals('[' + SMALLEST_POSSIBLE_JSON + ']', contentCollector.addAndReturnBatch("."));
    }

    @Test
    public void testAddAndReturnBatchTwoRecords() {
        timesConstructorCalled = 2;
        final String expected = '[' + SMALLEST_POSSIBLE_JSON + ',' + SMALLEST_POSSIBLE_JSON + ']';
        when(mockHttpPostConfigurationProvider.getMaxBytes()).thenReturn(Integer.toString(expected.length()));
        contentCollector = new ContentCollector(mockHttpPostConfigurationProvider);

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
