package com.expedia.www.haystack.pipes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemExitUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    static final String ERROR_MSG = "Thread [%s] is down because of an uncaught exception; "
            + "shutting down JVM so that Kubernetes can restart it";
    static final int SYSTEM_EXIT_STATUS = -1; // TODO what status should be used?

    static Logger logger = LoggerFactory.getLogger(SystemExitUncaughtExceptionHandler.class);
    static Factory factory = new Factory();

    @Override
    public void uncaughtException(Thread thread, Throwable throwable) {
        logger.error(String.format(ERROR_MSG, thread), throwable);
        factory.getRuntime().exit(SYSTEM_EXIT_STATUS);
    }

    static class Factory {
        Runtime getRuntime() {
            return Runtime.getRuntime();
        }
    }
}
