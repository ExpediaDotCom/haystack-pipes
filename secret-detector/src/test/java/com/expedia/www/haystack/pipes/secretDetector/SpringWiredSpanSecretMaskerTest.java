package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.www.haystack.commons.secretDetector.span.SpanNameAndCountRecorder;
import com.expedia.www.haystack.commons.secretDetector.span.SpanS3ConfigFetcher;
import com.expedia.www.haystack.commons.secretDetector.span.SpanSecretMasker;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
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
    private CountersAndTimer mockCountersAndTimer;
    @Mock
    private SpanNameAndCountRecorder mockSpanNameAndCountRecorder;

    private SpringWiredSpanSecretMasker springWiredSpanSecretMasker;

    @Before
    public void setUp() {
        springWiredSpanSecretMasker = new SpringWiredSpanSecretMasker(mockFinderEngine, mockSpanSecretMaskerFactory,
                mockS3ConfigFetcher, mockCountersAndTimer, mockSpanNameAndCountRecorder);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockFinderEngine);
        verifyNoMoreInteractions(mockSpanSecretMaskerFactory);
        verifyNoMoreInteractions(mockS3ConfigFetcher);
        verifyNoMoreInteractions(mockCountersAndTimer);
        verifyNoMoreInteractions(mockSpanNameAndCountRecorder);
    }

    @Test
    public void testApply() {
        springWiredSpanSecretMasker.apply(NO_TAGS_SPAN);

        verify(mockCountersAndTimer).recordSpanArrivalDelta(NO_TAGS_SPAN);
        verify(mockFinderEngine).findWithType(STRING_FIELD_VALUE);
        verify(mockFinderEngine).findWithType(BASE_64_DECODED_STRING);
    }
}
