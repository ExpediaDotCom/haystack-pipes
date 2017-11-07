package com.expedia.www.haystack.pipes.commons.serialization;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;

class SerializerDeserializerBase {
    static final String REQUEST_COUNTER_NAME = "REQUEST";
    static final String BYTES_IN_COUNTER_NAME = "BYTES_IN";
    static Factory factory = new Factory();
    static final Map<String, Counter> REQUESTS_COUNTERS = new ConcurrentHashMap<>();
    static final Map<String, Counter> BYTES_IN_COUNTERS = new ConcurrentHashMap<>();

    final String application;
    final Counter request;
    final Counter bytesIn;

    SerializerDeserializerBase(String application) {
        this.application = application.intern();
        request = getOrCreateCounter(REQUESTS_COUNTERS, REQUEST_COUNTER_NAME);
        bytesIn = getOrCreateCounter(BYTES_IN_COUNTERS, BYTES_IN_COUNTER_NAME);
    }

    Counter getOrCreateCounter(Map<String, Counter> counters, String counterName) {
        synchronized (application) {
            if (!counters.containsKey(application)) {
                final Counter newCounter = factory.createCounter(application, getClass().getSimpleName(), counterName);
                counters.put(application, newCounter);
            }
        }
        return counters.get(application);
    }

    Timer getOrCreateTimer(Map<String, Timer> timers, String timerName) {
        synchronized (application) {
            if (!timers.containsKey(application)) {
                final Timer newTimer = factory.createTimer(application, getClass().getSimpleName(), timerName);
                timers.put(application, newTimer);
            }
        }
        return timers.get(application);
    }

    static class Factory {
        static MetricObjects metricObjects = new MetricObjects();

        Counter createCounter(String application, String className, String counterName) {
            return metricObjects.createAndRegisterCounter(
                    SUBSYSTEM, application, className, counterName);
        }
        Timer createTimer(String application, String className, String timerName) {
            return metricObjects.createAndRegisterBasicTimer(
                    SUBSYSTEM, application, className, timerName, TimeUnit.MICROSECONDS);
        }
    }
}
