package com.expedia.www.haystack.pipes.commons;

import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;

public class CountersAndTimer {
    private final Timer timer;
    private final Counter requestCounter;
    private final Counter [] counters;

    public CountersAndTimer(Timer timer, Counter requestCounter, Counter...counters) {
        this.timer = timer;
        this.requestCounter = requestCounter;
        this.counters = counters;
    }

    public void incrementRequestCounter() {
        requestCounter.increment();
    }

    public void incrementCounter(int index) {
        counters[index].increment();
    }

    public void incrementCounter(int index, int value) {
        counters[index].increment(value);
    }

    public Stopwatch startTimer() {
        return timer.start();
    }
}
