/*
 * Copyright 2018 Expedia, Inc.
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

import com.expedia.open.tracing.SpanOrBuilder;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

public class TimersAndCounters {
    private static final int REQUEST_TIMER_INDEX = 0;
    private static final int SPAN_ARRIVAL_TIMER_INDEX = 1;
    private final Clock clock;
    private final Timers timers;
    private final Counter requestCounter;
    private final Counter [] counters;

    public TimersAndCounters(Clock clock,
                             Timers timers,
                             Counter requestCounter,
                             Counter...counters) {
        this.clock = clock;
        this.timers = timers;
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
        return timers.getTimer(REQUEST_TIMER_INDEX).start();
    }

    public void recordSpanArrivalDelta(SpanOrBuilder spanOrBuilder) {
        final long spanArrivalTimeMillis = (spanOrBuilder.getStartTime() + spanOrBuilder.getDuration()) / 1000L;
        if(spanArrivalTimeMillis > 0L) { // only emit this metric if the span is providing a sensible value
            final long now = clock.millis();
            final long delta = now - spanArrivalTimeMillis;
            timers.getTimer(SPAN_ARRIVAL_TIMER_INDEX).record(delta, TimeUnit.MILLISECONDS);
        }
    }

}
