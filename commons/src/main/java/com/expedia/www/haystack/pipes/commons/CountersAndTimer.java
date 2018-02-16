package com.expedia.www.haystack.pipes.commons;

import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;

public class CountersAndTimer {
    private final Counter requestCounter;
    private final Counter secondCounter;
    private final Timer timer;

    public CountersAndTimer(Counter requestCounter, Counter secondCounter, Timer timer) {
        this.requestCounter = requestCounter;
        this.secondCounter = secondCounter;
        this.timer = timer;
    }

    public void incrementRequestCounter() {
        requestCounter.increment();
    }

    public void incrementSecondCounter() {
        secondCounter.increment();
    }

    public void incrementSecondCounter(int value) {
        secondCounter.increment(value);
    }

    public Stopwatch startTimer() {
        return timer.start();
    }
}
