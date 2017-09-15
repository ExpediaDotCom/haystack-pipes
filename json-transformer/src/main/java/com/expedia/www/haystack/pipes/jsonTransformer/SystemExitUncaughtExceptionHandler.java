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
package com.expedia.www.haystack.pipes.jsonTransformer;

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
