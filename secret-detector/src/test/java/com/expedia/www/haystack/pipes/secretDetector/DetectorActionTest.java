/*
 * Copyright 2018 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.expedia.www.haystack.pipes.secretDetector.config.ActionsConfigurationProvider;
import com.expedia.www.haystack.pipes.secretDetector.actions.DetectedAction;
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
import java.util.Map;

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

    @Mock
    private CountersAndTimer mockCountersAndTimer;
    @Mock
    private Detector mockDetector;
    @Mock
    private Logger mockLogger;
    @Mock
    private Stopwatch mockTimer;
    @Mock
    private ActionsConfigurationProvider mockActionsConfigurationProvider;
    @Mock
    private DetectedAction mockDetectedAction;

    private DetectorAction detectorAction;
    private List<DetectedAction> detectedActions;

    @Before
    public void setUp() {
        detectedActions = Collections.singletonList(mockDetectedAction);
        detectorAction = new DetectorAction(
                mockCountersAndTimer, mockDetector, mockLogger, mockActionsConfigurationProvider);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockCountersAndTimer, mockDetector, mockLogger, mockTimer,
                mockActionsConfigurationProvider, mockDetectedAction);
    }

    @Test
    public void testApplyNoSecrets() {
        whensForApply(Collections.emptyMap());

        detectorAction.apply(KEY, FULLY_POPULATED_SPAN);

        verifiesForApply();
    }

    @Test
    public void testApplyOneSecretFound() {
        final Map<String, List<String>> secrets = Collections.singletonMap(KEY, Collections.singletonList(KEY));
        whensForApply(secrets);
        when(mockActionsConfigurationProvider.getDetectedActions()).thenReturn(detectedActions);

        detectorAction.apply(KEY, FULLY_POPULATED_SPAN);

        verifiesForApply();
        verify(mockLogger).info(
                String.format(CONFIDENTIAL_DATA_MSG, SERVICE_NAME, OPERATION_NAME, SPAN_ID, TRACE_ID, secrets, 0));
        verify(mockActionsConfigurationProvider).getDetectedActions();
        verify(mockDetectedAction).send(FULLY_POPULATED_SPAN, secrets);
    }

    private void whensForApply(Map<String, List<String>> secrets) {
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
