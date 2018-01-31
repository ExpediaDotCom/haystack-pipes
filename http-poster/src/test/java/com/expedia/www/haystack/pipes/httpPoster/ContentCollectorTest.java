package com.expedia.www.haystack.pipes.httpPoster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.httpPoster.HttpPostConfigurationProviderTest.LARGEST_POSSIBLE_MAX_BYTES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
    private HttpPostConfigurationProvider mockHttpPostConfigurationProvider;

    private ContentCollector contentCollector;
    private int timesConstructorCalled = 1;

    @Before
    public void setUp() {
        when(mockHttpPostConfigurationProvider.maxbytes()).thenReturn(LARGEST_POSSIBLE_MAX_BYTES);
        when(mockHttpPostConfigurationProvider.separator()).thenReturn(SEPARATOR);
        when(mockHttpPostConfigurationProvider.bodyprefix()).thenReturn(BODY_PREFIX);
        when(mockHttpPostConfigurationProvider.bodysuffix()).thenReturn(BODY_SUFFIX);
        contentCollector = new ContentCollector(mockHttpPostConfigurationProvider);
    }

    @After
    public void tearDown() {
        verify(mockHttpPostConfigurationProvider, times(timesConstructorCalled)).maxbytes();
        verify(mockHttpPostConfigurationProvider, times(timesConstructorCalled)).separator();
        verify(mockHttpPostConfigurationProvider, times(timesConstructorCalled)).bodyprefix();
        verify(mockHttpPostConfigurationProvider, times(timesConstructorCalled)).bodysuffix();
        verifyNoMoreInteractions(mockHttpPostConfigurationProvider);
    }

    @Test
    public void testAddAndReturnBatchOneRecord() {
        timesConstructorCalled = 2;
        when(mockHttpPostConfigurationProvider.maxbytes()).thenReturn(SMALLEST_POSSIBLE_MAX_BYTES);
        contentCollector = new ContentCollector(mockHttpPostConfigurationProvider);

        assertEquals("", contentCollector.addAndReturnBatch(SMALLEST_POSSIBLE_JSON));
        assertEquals('[' + SMALLEST_POSSIBLE_JSON + ']', contentCollector.addAndReturnBatch("."));
    }

    @Test
    public void testAddAndReturnBatchTwoRecords() {
        timesConstructorCalled = 2;
        final String expected = '[' + SMALLEST_POSSIBLE_JSON + ',' + SMALLEST_POSSIBLE_JSON + ']';
        when(mockHttpPostConfigurationProvider.maxbytes()).thenReturn(expected.length());
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
