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
package com.expedia.www.haystack.pipes.kafka;

import com.expedia.www.haystack.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.Timers;
import com.expedia.www.haystack.pipes.commons.TimersAndCounters;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.HealthStatusListener;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.key.extractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.commons.key.extractor.loader.SpanKeyExtractorLoader;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import com.netflix.servo.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.support.SpringBootServletInitializer;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SPAN_ARRIVAL_TIMER_NAME;
import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.kafka.Constants.APPLICATION;

/**
 * A very simple Spring Boot application that is intended to support only a single REST endpoint (index.html)
 * to indicate that the JVM is running.
 */

public class Service extends SpringBootServletInitializer {
    /*    // Singleton, initialized on first constructor call, so that future instances created by Spring during unit tests
        // will not overwrite the initial INSTANCE (with mocks) created by the unit tests.
        @VisibleForTesting
        static final AtomicReference<Service> INSTANCE = new AtomicReference<>();*/
    @VisibleForTesting
    static final String STARTUP_MSG = "Starting FirehoseIsActiveController";

    //    private final ProtobufToKafkaProducer protobufToKafkaProducer;
    private static final Logger logger = LoggerFactory.getLogger(Service.class);
    private static final MetricObjects metricObjects = new MetricObjects();

//    @Autowired
//    Service(ProtobufToKafkaProducer protobufToKafkaProducer,
//            Factory appFactory,
//            Logger appLogger) {
//        this.protobufToKafkaProducer = protobufToKafkaProducer;
//        this.factory = appFactory;
//        INSTANCE.compareAndSet(null, this);
//
//    }

    public static void main(String[] args) {
        logger.info(STARTUP_MSG);
        startService();
    }

    private static void startService() {
        SerdeFactory serdeFactory = new SerdeFactory();
        KafkaConfigurationProvider kafkaConfigurationProvider = new com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider();
        ProjectConfiguration projectConfiguration = new ProjectConfiguration();
        KafkaToKafkaPipeline kafkaToKafkaPipeline = prepareKafkaToKafkaPipeline(projectConfiguration);
        final HealthController healthController = new HealthController();
//        healthController.addListener((HealthStatusListener) new UpdateHealthStatusFile("/app/isHealthy")); // TODO should come from config

        KafkaStreamStarter kafkaStreamStarter = new KafkaStreamStarter(ProtobufToKafkaProducer.class, APPLICATION, healthController);

        ProtobufToKafkaProducer protobufToKafkaProducer = new ProtobufToKafkaProducer(
                kafkaStreamStarter, serdeFactory, kafkaToKafkaPipeline, kafkaConfigurationProvider);
        protobufToKafkaProducer.main();
    }


    private static KafkaToKafkaPipeline prepareKafkaToKafkaPipeline(ProjectConfiguration projectConfiguration) {
        List<SpanKeyExtractor> spanKeyExtractors = SpanKeyExtractorLoader.getInstance().getSpanKeyExtractor();
        TimersAndCounters timersAndCounters = getTimersAndCounter();
        return new KafkaToKafkaPipeline(timersAndCounters, projectConfiguration, spanKeyExtractors);
    }

    private static TimersAndCounters getTimersAndCounter() {

        Timer kafkaProducerPost = metricObjects.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                KafkaToKafkaPipeline.class.getSimpleName(), "KAFKA_PRODUCER_POST", TimeUnit.MICROSECONDS);
        Timer spanArrivalTimer = metricObjects.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                KafkaToKafkaPipeline.class.getSimpleName(), SPAN_ARRIVAL_TIMER_NAME, TimeUnit.MILLISECONDS);
        Timers timers = new Timers(kafkaProducerPost, spanArrivalTimer);

        Clock clock = Clock.systemUTC();
        Counter kafkaToKafkaRequestCounter = metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                KafkaToKafkaPipeline.class.getSimpleName(), "REQUEST");
        Counter postsInFlightCounter = metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                KafkaToKafkaPipeline.class.getSimpleName(), "POSTS_IN_FLIGHT");
        return new TimersAndCounters(clock, timers,
                kafkaToKafkaRequestCounter, postsInFlightCounter);
    }

}
