/*
 * Copyright 2020 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.kafka.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/*
 This class handles request, success and failure metrics for kafka producer
 */
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
