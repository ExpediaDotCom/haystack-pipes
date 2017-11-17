/*
 * Copyright 2017 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.commons;

import com.expedia.www.haystack.pipes.commons.SystemExitUncaughtExceptionHandler.Factory;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.expedia.www.haystack.pipes.commons.SystemExitUncaughtExceptionHandler.ERROR_MSG;
import static com.expedia.www.haystack.pipes.commons.SystemExitUncaughtExceptionHandler.LOG4J_METHOD_NAME;
import static com.expedia.www.haystack.pipes.commons.SystemExitUncaughtExceptionHandler.LOGBACK_METHOD_NAME;
import static com.expedia.www.haystack.pipes.commons.SystemExitUncaughtExceptionHandler.SYSTEM_EXIT_STATUS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SystemExitUncaughtExceptionHandlerTest {
    private static final String METHOD_NAME_STOP = "stop";
    private static final String METHOD_NAME_CLOSE = "close";
    private static final String METHOD_NAME_TO_STRING = "toString";
    @Mock
    private Logger mockLogger;
    private Logger realLogger;

    @Mock
    private Factory mockFactory;
    private Factory realFactory;

    @Mock
    private Runtime mockRuntime;

    @Mock
    private KafkaStreams mockKafkaStreams;

    @Mock
    private ILoggerFactory mockILoggerFactory;

    private Throwable throwable;
    private SystemExitUncaughtExceptionHandler systemExitUncaughtExceptionHandler;

    @Before
    public void setUp() {
        systemExitUncaughtExceptionHandler = new SystemExitUncaughtExceptionHandler(mockKafkaStreams);
        realLogger = SystemExitUncaughtExceptionHandler.logger;
        SystemExitUncaughtExceptionHandler.logger = mockLogger;
        realFactory = SystemExitUncaughtExceptionHandler.factory;
        SystemExitUncaughtExceptionHandler.factory = mockFactory;
        throwable = new Throwable();
    }

    @After
    public void tearDown() {
        SystemExitUncaughtExceptionHandler.logger = realLogger;
        SystemExitUncaughtExceptionHandler.factory = realFactory;
        verifyNoMoreInteractions(mockLogger, mockFactory, mockRuntime, mockKafkaStreams, mockILoggerFactory);
    }

    @Test(expected = NullPointerException.class)
    public void testNullKafkaStreams() {
        try {
            new SystemExitUncaughtExceptionHandler(null);
        } catch(NullPointerException e) {
            assertEquals(SystemExitUncaughtExceptionHandler.KAFKA_STREAMS_IS_NULL, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testUncaughtException() {
        when(mockFactory.getRuntime()).thenReturn(mockRuntime);
        when(mockFactory.getILoggerFactory()).thenReturn(mockILoggerFactory);
        final Thread thread = Thread.currentThread();

        systemExitUncaughtExceptionHandler.uncaughtException(thread, throwable);

        verify(mockLogger).error(String.format(ERROR_MSG, thread), throwable);
        verify(mockFactory).getRuntime();
        verify(mockFactory).getILoggerFactory();
        verify(mockRuntime).exit(SYSTEM_EXIT_STATUS);
        verify(mockKafkaStreams).close();
    }

    @Test
    public void testFactoryGetRuntime() {
        final Runtime runtime = realFactory.getRuntime();

        assertSame(Runtime.getRuntime(), runtime);
    }

    @Test
    public void testLogbackMethodNameIsStop() {
        assertEquals(METHOD_NAME_STOP, LOGBACK_METHOD_NAME);
    }

    @Test
    public void testLog4jMethodNameIsClose() {
        assertEquals(METHOD_NAME_CLOSE, LOG4J_METHOD_NAME);
    }

    @Test
    public void testShutdownLoggerNoSuccessfulCallMade() {
        assertNull(testShutdownLogger(LOGBACK_METHOD_NAME, LOG4J_METHOD_NAME));
    }

    @Test
    public void testShutdownLoggerLogbackCall() {
        assertEquals(mockILoggerFactory.toString(), testShutdownLogger(METHOD_NAME_TO_STRING, LOG4J_METHOD_NAME));
    }

    @Test
    public void testShutdownLoggerLog4jCall() {
        assertEquals(mockILoggerFactory.toString(), testShutdownLogger(LOGBACK_METHOD_NAME, METHOD_NAME_TO_STRING));
    }

    private Object testShutdownLogger(String logbackMethodName, String log4jMethodName) {
        when(mockFactory.getILoggerFactory()).thenReturn(mockILoggerFactory);

        final Object object = systemExitUncaughtExceptionHandler.shutdownLogger(logbackMethodName, log4jMethodName);

        verify(mockFactory).getILoggerFactory();
        return object;
    }

    @Test
    public void testFactoryGetILoggerFactory() {
        final ILoggerFactory iLoggerFactory = realFactory.getILoggerFactory();

        assertSame(LoggerFactory.getILoggerFactory(), iLoggerFactory);
    }
}
