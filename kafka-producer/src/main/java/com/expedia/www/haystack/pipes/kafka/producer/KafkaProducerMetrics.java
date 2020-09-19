package com.expedia.www.haystack.pipes.kafka.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class KafkaProducerMetrics {

    private String name;
    private Counter successCounter;
    private Counter failureCounter;
    private Counter requestCounter;
    private Timer timer;

    public KafkaProducerMetrics(String name, MetricRegistry metricRegistry) {
        this.name = name;
        this.requestCounter = metricRegistry.counter(name + "_requests_counter");
        this.successCounter = metricRegistry.counter(name + "_success_counter");
        this.failureCounter = metricRegistry.counter(name + "_failure_counter");
        this.timer = metricRegistry.timer(name + "_timer");
    }

    public void incSuccessCounter() {
        this.successCounter.inc();
    }

    public void incFailureCounter() {
        this.failureCounter.inc();
    }

    public void incRequestCounter() {
        this.requestCounter.inc();
    }

    public Timer getTimer() {
        return timer;
    }
}
