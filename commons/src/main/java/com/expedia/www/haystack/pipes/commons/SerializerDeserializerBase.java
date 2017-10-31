package com.expedia.www.haystack.pipes.commons;

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
    static MetricObjects metricObjects = new MetricObjects();

    final String application;
    final Counter request;
    final Counter bytesIn;

    SerializerDeserializerBase(String application) {
        this.application = application.intern();
        request = getOrCreateCounter(REQUESTS_COUNTERS, REQUEST_COUNTER_NAME);
        bytesIn = getOrCreateCounter(BYTES_IN_COUNTERS, BYTES_IN_COUNTER_NAME);
    }

    private Counter getOrCreateCounter(Map<String, Counter> counters, String counterName) {
        if (!counters.containsKey(this.application)) {
            final Counter newCounter = factory.createCounter(this.application, getClass().getSimpleName(), counterName);
            counters.put(this.application, newCounter);
        }
        return counters.get(this.application);
    }

    Timer getOrCreateTimer(Map<String, Timer> timers, String timerName) {
        if (!timers.containsKey(this.application)) {
            final Timer newTimer = factory.createTimer(this.application, getClass().getSimpleName(), timerName);
            timers.put(this.application, newTimer);
        }
        return timers.get(this.application);
    }

    static class Factory {
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
