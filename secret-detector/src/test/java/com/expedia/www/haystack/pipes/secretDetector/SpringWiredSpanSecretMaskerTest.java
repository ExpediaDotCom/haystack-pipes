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

import com.expedia.www.haystack.commons.secretDetector.span.SpanNameAndCountRecorder;
import com.expedia.www.haystack.commons.secretDetector.span.SpanS3ConfigFetcher;
import com.expedia.www.haystack.commons.secretDetector.span.SpanSecretMasker;
import com.expedia.www.haystack.pipes.commons.TimersAndCounters;
import io.dataapps.chlorine.finder.FinderEngine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BASE_64_DECODED_STRING;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.NO_TAGS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.STRING_FIELD_VALUE;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class SpringWiredSpanSecretMaskerTest {
    @Mock
    private FinderEngine mockFinderEngine;
    @Mock
    private SpanSecretMasker.Factory mockSpanSecretMaskerFactory;
    @Mock
    private SpanS3ConfigFetcher mockS3ConfigFetcher;
    @Mock
    private TimersAndCounters mockTimersAndCounters;
    @Mock
    private SpanNameAndCountRecorder mockSpanNameAndCountRecorder;

    private SpringWiredSpanSecretMasker springWiredSpanSecretMasker;

    @Before
    public void setUp() {
        springWiredSpanSecretMasker = new SpringWiredSpanSecretMasker(mockFinderEngine, mockSpanSecretMaskerFactory,
                mockS3ConfigFetcher, mockTimersAndCounters, mockSpanNameAndCountRecorder);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockFinderEngine);
        verifyNoMoreInteractions(mockSpanSecretMaskerFactory);
        verifyNoMoreInteractions(mockS3ConfigFetcher);
        verifyNoMoreInteractions(mockTimersAndCounters);
        verifyNoMoreInteractions(mockSpanNameAndCountRecorder);
    }

    @Test
    public void testApply() {
        springWiredSpanSecretMasker.apply(NO_TAGS_SPAN);

        verify(mockTimersAndCounters).recordSpanArrivalDelta(NO_TAGS_SPAN);
        verify(mockFinderEngine).findWithType(STRING_FIELD_VALUE);
        verify(mockFinderEngine).findWithType(BASE_64_DECODED_STRING);
    }
}
