package com.expedia.www.haystack.pipes.kafkaProducer;

import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CountersAndTimer {
    private final Counter requestCounter;
    private final Counter postsInFlightCounter;
    private final Timer kafkaProducerPost;

    @Autowired
    public CountersAndTimer(Counter produceIntoExternalKafkaActionRequestCounter,
                            Counter postsInFlightCounter,
                            Timer kafkaProducerPost) {
        this.requestCounter = produceIntoExternalKafkaActionRequestCounter;
        this.postsInFlightCounter = postsInFlightCounter;
        this.kafkaProducerPost = kafkaProducerPost;
    }

    public void incrementRequestCounter() {
        requestCounter.increment();
    }

    public Stopwatch startTimer() {
        return kafkaProducerPost.start();
    }

    public void incrementPostsInFlightCounter() {
        postsInFlightCounter.increment();
    }
}
