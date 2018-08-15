package com.expedia.www.haystack.pipes.commons;

import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

public class CountersAndTimer {
    private final Clock clock;
    private final Timer timer;
    private final Timer spanArrivalTimer;
    private final Counter requestCounter;
    private final Counter [] counters;

    public CountersAndTimer(Clock clock,
                            Timer timer,
                            Timer spanArrivalTimer,
                            Counter requestCounter,
                            Counter...counters) {
        this.clock = clock;
        this.timer = timer;
        this.spanArrivalTimer = spanArrivalTimer;
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

    public void recordSpanArrivalDelta(long spanArrivalTimeMillis) {
        final long now = clock.millis();
        final long delta = now - spanArrivalTimeMillis;
        spanArrivalTimer.record(delta, TimeUnit.MILLISECONDS);
    }

}
