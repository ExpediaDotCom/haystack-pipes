package com.expedia.www.haystack.pipes.httpPoster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Random;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentCollectorTest {
    private static final Random RANDOM = new Random();
    private static final int MAX_BYTES = RANDOM.nextInt(Integer.MAX_VALUE);

    @Mock
    private HttpPostConfigurationProvider mockHttpPostConfigurationProvider;

    private ContentCollector contentCollector;

    @Before
    public void setUp() {
        when(mockHttpPostConfigurationProvider.maxbytes()).thenReturn(MAX_BYTES);
        contentCollector = new ContentCollector(mockHttpPostConfigurationProvider);
    }

    @After
    public void tearDown() {
        verify(mockHttpPostConfigurationProvider).maxbytes();
        verifyNoMoreInteractions(mockHttpPostConfigurationProvider);
    }

    @Test
    public void doNothingTest() {
        assertNotNull(contentCollector);
    }

}
