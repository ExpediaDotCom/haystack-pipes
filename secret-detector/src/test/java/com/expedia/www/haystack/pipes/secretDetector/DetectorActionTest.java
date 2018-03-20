package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.netflix.servo.monitor.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.OPERATION_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.SERVICE_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.SPAN_ID;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.TRACE_ID;
import static com.expedia.www.haystack.pipes.secretDetector.DetectorAction.CONFIDENTIAL_DATA_MSG;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DetectorActionTest {
    private static final String KEY = RANDOM.nextLong() + "KEY";
    private static final String SECRET = RANDOM.nextLong() + "SECRET";

    @Mock
    private CountersAndTimer mockCountersAndTimer;
    @Mock
    private Detector mockDetector;
    @Mock
    private Logger mockLogger;
    @Mock
    private Stopwatch mockTimer;

    private DetectorAction detectorAction;

    @Before
    public void setUp() {
        detectorAction = new DetectorAction(mockCountersAndTimer, mockDetector, mockLogger);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockCountersAndTimer, mockDetector, mockLogger, mockTimer);
    }

    @Test
    public void testApplyNoSecrets() {
        whensForApply(Collections.emptyList());

        detectorAction.apply(KEY, FULLY_POPULATED_SPAN);

        verifiesForApply();
    }

    @Test
    public void testApplyOneSecretFound() {
        whensForApply(Collections.singletonList(SECRET));

        detectorAction.apply(KEY, FULLY_POPULATED_SPAN);

        verifiesForApply();
        verify(mockLogger).info(String.format(CONFIDENTIAL_DATA_MSG, SERVICE_NAME, OPERATION_NAME, SPAN_ID, TRACE_ID));
    }

    private void whensForApply(List<String> secrets) {
        when(mockCountersAndTimer.startTimer()).thenReturn(mockTimer);
        when(mockDetector.findSecrets(any(Span.class))).thenReturn(secrets);
    }

    private void verifiesForApply() {
        verify(mockCountersAndTimer).incrementRequestCounter();
        verify(mockCountersAndTimer).startTimer();
        verify(mockDetector).findSecrets(FULLY_POPULATED_SPAN);
        verify(mockTimer).stop();
    }
}
