package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Counter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Random;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class HttpPostActionTest {
    private final static Random RANDOM = new Random();
    private final static String KEY = RANDOM.nextLong() + "KEY";

    @Mock
    private ContentCollector mockContentCollector;
    @Mock
    private MetricObjects mockMetricObjects;
    @Mock
    private Printer mockPrinter;
    @Mock
    private Counter mockRequestCounter;

    private HttpPostAction httpPostExternalAction;

    @Before
    public void setUp() {
        httpPostExternalAction = new HttpPostAction(mockContentCollector, mockMetricObjects, mockPrinter,
                mockRequestCounter);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockContentCollector, mockMetricObjects, mockPrinter, mockRequestCounter);
    }

    @Test
    public void testApply() {
        httpPostExternalAction.apply(KEY, FULLY_POPULATED_SPAN);

        verify(mockRequestCounter).increment();
    }
}
