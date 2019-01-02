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
package com.expedia.www.haystack.pipes.firehoseWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.firehoseWriter.UnexpectedExceptionLogger.UNEXPECTED_EXCEPTION_MSG;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class UnexpectedExceptionLoggerTest {

    @Mock
    private Logger mockLogger;
    @Mock
    private Exception mockException;

    private UnexpectedExceptionLogger unexpectedExceptionLogger;

    @Before
    public void setUp() {
        unexpectedExceptionLogger = new UnexpectedExceptionLogger(mockLogger);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockException);
    }

    @Test
    public void testLogError() {
        unexpectedExceptionLogger.logError(mockException);

        verify(mockLogger).error(UNEXPECTED_EXCEPTION_MSG, mockException);
    }

}
