package com.expedia.www.haystack.pipes;

import com.expedia.www.haystack.pipes.SystemExitUncaughtExceptionHandler.Factory;
import com.expedia.www.haystack.pipes.SystemExitUncaughtExceptionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.SystemExitUncaughtExceptionHandler.ERROR_MSG;
import static com.expedia.www.haystack.pipes.SystemExitUncaughtExceptionHandler.SYSTEM_EXIT_STATUS;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SystemExitUncaughtExceptionHandlerTest {
    @Mock
    private Logger mockLogger;
    private Logger realLogger;

    @Mock
    private Factory mockFactory;
    private Factory realFactory;

    @Mock
    private Runtime mockRuntime;

    private Throwable throwable;
    private SystemExitUncaughtExceptionHandler systemExitUncaughtExceptionHandler;

    @Before
    public void setUp() {
        systemExitUncaughtExceptionHandler = new SystemExitUncaughtExceptionHandler();
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
        verifyNoMoreInteractions(mockLogger, mockFactory, mockRuntime);
    }

    @Test
    public void testUncaughtException() {
        when(mockFactory.getRuntime()).thenReturn(mockRuntime);
        final Thread thread = Thread.currentThread();

        systemExitUncaughtExceptionHandler.uncaughtException(thread, throwable);

        verify(mockLogger).error(String.format(ERROR_MSG, thread), throwable);
        verify(mockFactory).getRuntime();
        verify(mockRuntime).exit(SYSTEM_EXIT_STATUS);
    }

    @Test
    public void testFactoryGetRuntime() {
        final Runtime runtime = realFactory.getRuntime();

        assertSame(Runtime.getRuntime(), runtime);
    }
}
