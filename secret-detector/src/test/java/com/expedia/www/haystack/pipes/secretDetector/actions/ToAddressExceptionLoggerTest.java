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
package com.expedia.www.haystack.pipes.secretDetector.actions;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.secretDetector.actions.ToAddressExceptionLogger.TO_ADDRESS_EXCEPTION_MSG;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ToAddressExceptionLoggerTest {
    private static final List<String> TOS = Collections.singletonList(RANDOM.nextLong() + "TOS");
    private static final String MESSAGE = RANDOM.nextLong() + "MESSAGE";

    @Mock
    private Logger mockLogger;
    @Mock
    private Exception mockException;

    private ToAddressExceptionLogger toAddressExceptionLogger;

    @Before
    public void setUp() {
        toAddressExceptionLogger = new ToAddressExceptionLogger(mockLogger);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockException);
    }

    @Test
    public void testLogError() {
        when(mockException.getMessage()).thenReturn(MESSAGE);

        toAddressExceptionLogger.logError(TOS, mockException);

        final String message = String.format(TO_ADDRESS_EXCEPTION_MSG, TOS);
        verify(mockLogger).error(message, mockException);
    }

}
