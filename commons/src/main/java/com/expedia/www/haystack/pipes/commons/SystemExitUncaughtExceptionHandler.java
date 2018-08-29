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

import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.Validate;

public class SystemExitUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    @VisibleForTesting
    static final String ERROR_MSG = "Thread [%s] is down because of an uncaught exception; "
            + "shutting down JVM so that Kubernetes can restart it";
    @VisibleForTesting
    static final String KAFKA_STREAMS_IS_NULL = "kafkaStreams is null";
    @VisibleForTesting
    static final int SYSTEM_EXIT_STATUS = -1;

    @VisibleForTesting
    static final String LOGBACK_METHOD_NAME = "stop";
    @VisibleForTesting
    static final String LOG4J_METHOD_NAME = "close";

    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(SystemExitUncaughtExceptionHandler.class);
    @VisibleForTesting
    static Factory factory = new Factory();

    private final HealthController healthController;

    public SystemExitUncaughtExceptionHandler(KafkaStreams kafkaStreams, HealthController healthController) {
        Validate.notNull(kafkaStreams, KAFKA_STREAMS_IS_NULL);
        this.healthController = healthController;
    }

    @Override
    public void uncaughtException(Thread thread, Throwable throwable) {
        logger.error(String.format(ERROR_MSG, thread), throwable);
        shutdownLogger(LOGBACK_METHOD_NAME, LOG4J_METHOD_NAME);
        healthController.setUnhealthy();
    }

    @VisibleForTesting
    Object shutdownLogger(String logbackMethodName, String log4jMethodName) {
        // See https://jira.qos.ch/browse/SLF4J-192
        ILoggerFactory iLoggerFactory = factory.getILoggerFactory();
        Class<?> clazz = iLoggerFactory.getClass();
        try {
            return clazz.getMethod(logbackMethodName).invoke(iLoggerFactory);
        } catch (ReflectiveOperationException ex) {
            try {
                return clazz.getMethod(log4jMethodName).invoke(iLoggerFactory);
            } catch (ReflectiveOperationException ignored) {
                // ignored
            }
        }
        return null;
    }

    @VisibleForTesting
    static class Factory {
        Runtime getRuntime() {
            return Runtime.getRuntime();
        }

        ILoggerFactory getILoggerFactory() {
            return LoggerFactory.getILoggerFactory();
        }
    }
}
