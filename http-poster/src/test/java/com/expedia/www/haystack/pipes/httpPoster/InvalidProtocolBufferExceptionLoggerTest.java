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
package com.expedia.www.haystack.pipes.httpPoster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.PROTOBUF_ERROR_MSG;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.NO_TAGS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InvalidProtocolBufferExceptionLoggerTest {
    private static final String MESSAGE = RANDOM.nextLong() + "MESSAGE";

    @Mock
    private Logger mockLogger;
    @Mock
    private Exception mockException;

    private InvalidProtocolBufferExceptionLogger invalidProtocolBufferExceptionLogger;

    @Before
    public void setUp() {
        invalidProtocolBufferExceptionLogger = new InvalidProtocolBufferExceptionLogger(mockLogger);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockException);
    }

    @Test
    public void testLogError() {
        when(mockException.getMessage()).thenReturn(MESSAGE);

        invalidProtocolBufferExceptionLogger.logError(NO_TAGS_SPAN, mockException);

        verify(mockException).getMessage();
        final String msg = String.format(PROTOBUF_ERROR_MSG, NO_TAGS_SPAN.toString(), MESSAGE);
        final String expected = "Exception printing Span [" + NO_TAGS_SPAN.toString() + "]"
                + "; received message [" + MESSAGE + "]";
        assertEquals(expected, msg);
        verify(mockLogger).error(msg, mockException);
    }

}
